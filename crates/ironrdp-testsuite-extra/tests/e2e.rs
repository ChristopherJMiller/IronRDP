// FIXME: tests in this module can probably be rewritten to be much shorter using the ironrdp-client crate.

use core::net::SocketAddr;
use core::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use core::time::Duration;
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use ironrdp::connector;
use ironrdp::core::{Encode as _, encode_vec, impl_as_any};
use ironrdp::dvc::DrdynvcClient;
use ironrdp::echo::client::EchoClient;
use ironrdp::pdu::bitmap::{BitmapData, BitmapUpdateData, Compression};
use ironrdp::pdu::fast_path::{EncryptionFlags, FastPathHeader, FastPathUpdatePdu, Fragmentation, UpdateCode};
use ironrdp::pdu::geometry::InclusiveRectangle;
use ironrdp::pdu::rdp::capability_sets::MajorPlatformType;
use ironrdp::pdu::rdp::client_info::CompressionType as PduCompressionType;
use ironrdp::pdu::rdp::headers::CompressionFlags;
use ironrdp::pdu::rdp::server_error_info::{ErrorInfo, ProtocolIndependentCode};
use ironrdp::pdu::{self, gcc};
use ironrdp::server::{
    self, Acceptor, BitmapUpdate, DesktopSize, DisplayUpdate, KeyboardEvent, MouseEvent, PixelFormat, RdpServer,
    RdpServerDisplay, RdpServerDisplayUpdates, RdpServerInputHandler, ServerEvent, ServerResult, StaticChannelFactory,
    TlsIdentityCtx,
};
use ironrdp::session::image::DecodedImage;
use ironrdp::session::{self, ActiveStage, ActiveStageBuilder, ActiveStageOutput};
use ironrdp::svc::{StaticChannelSet, SvcMessage, SvcProcessor, SvcServerProcessor};
use ironrdp_async::{Framed, FramedWrite as _};
use ironrdp_bulk::{BulkCompressor, CompressionType as BulkCompressionType, flags as bulk_flags};
use ironrdp_dvc::{DvcClientProcessor, DvcEncode, DvcMessage, DvcProcessor};
use ironrdp_egfx::pdu::{CapabilitiesAdvertisePdu, CapabilitiesV8Flags, CapabilitySet, GfxPdu};
use ironrdp_rdpdr::pdu::RdpdrPdu;
use ironrdp_rdpdr::pdu::efs::{
    Capabilities, CoreCapability, CoreCapabilityKind, DeviceCreateResponse, DeviceIoRequest, DeviceIoResponse,
    MajorFunction, MinorFunction, NtStatus, ServerDeviceAnnounceResponse, ServerDriveIoRequest, VERSION_MAJOR,
    VERSION_MINOR_12, VersionAndIdPdu, VersionAndIdPduKind,
};
use ironrdp_rdpdr::pdu::esc::{ScardCall, ScardIoCtlCode};
use ironrdp_rdpdr::{Rdpdr, RdpdrBackend, RdpdrBackendFactory, RdpdrBackendProduct, RdpdrDrive};
use ironrdp_server::{GfxServerFactory, ServerEventSender};
use ironrdp_testsuite_extra as _;
use ironrdp_tls::TlsStream;
use ironrdp_tokio::TokioStream;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, oneshot};
use tracing::debug;

const DESKTOP_WIDTH: u16 = 1024;
const DESKTOP_HEIGHT: u16 = 768;
const USERNAME: &str = "";
const PASSWORD: &str = "";
const RDPDR_DEVICE_ID: u32 = 1;
const RDPDR_CLIENT_ID: u32 = 0x1234;
const RDPDR_COMPLETION_ID: u32 = 1;
#[cfg(windows)]
const RDPDR_READ_LENGTH: u32 = 32 * 1024;

#[tokio::test]
async fn test_client_server() {
    client_server(
        default_client_config(),
        |stage, _activation_factory, framed, _display_tx| async { (stage, framed) },
    )
    .await
}

/// Configuring UDP multitransport on the server must not disturb a client
/// that never advertises support for it: `set_multitransport_offer` gates on
/// the client's own GCC `MultiTransportChannelData` reciprocating, so with
/// `multitransport_flags: None` (the default) the acceptor never sends the
/// Initiate Multitransport Request and the connection proceeds exactly as it
/// does with no UDP transport configured at all.
#[tokio::test]
async fn test_client_server_with_udp_transport_configured_but_unused() {
    client_server_with_connector(
        default_client_config(),
        Vec::new(),
        Some(([127, 0, 0, 1], 0).into()),
        |connector| connector,
        |stage, _activation_factory, framed, _display_tx, _echo_handle| async { (stage, framed) },
    )
    .await;
}

/// Advertising the Graphics Pipeline early-capability bit must not disturb connection establishment.
///
/// The core testsuite separately decodes the emitted Connect Initial PDU and verifies the capability flag.
#[tokio::test]
async fn test_client_server_advertising_dyn_vc_gfx_protocol() {
    client_server(
        connector::Config {
            support_dyn_vc_gfx_protocol: true,
            ..default_client_config()
        },
        |stage, _activation_factory, framed, _display_tx| async { (stage, framed) },
    )
    .await
}

#[tokio::test]
async fn test_deactivation_reactivation() {
    let client_config = default_client_config();
    let mut image = DecodedImage::new(
        PixelFormat::RgbA32,
        client_config.desktop_size.width,
        client_config.desktop_size.height,
    );
    client_server(
        client_config,
        |mut stage, activation_factory, mut framed, display_tx| async move {
            display_tx
                .send(DisplayUpdate::Resize(DesktopSize {
                    width: 2048,
                    height: 2048,
                }))
                .unwrap();
            {
                let (action, payload) = framed.read_pdu().await.expect("valid PDU");
                let outputs = stage.process(&mut image, action, &payload).expect("stage process");
                let out = outputs.into_iter().next().unwrap();
                match out {
                    ActiveStageOutput::DeactivateAll => {
                        // TODO: factor this out in common client code
                        // Execute the Deactivation-Reactivation Sequence:
                        // https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-rdpbcgr/dfc234ce-481a-4674-9a5d-2a7bafb14432
                        debug!("Received Server Deactivate All PDU, executing Deactivation-Reactivation Sequence");
                        let mut connection_activation = activation_factory.create();
                        let mut buf = pdu::WriteBuf::new();
                        'activation_seq: loop {
                            let written = ironrdp_async::single_sequence_step_read(
                                &mut framed,
                                &mut connection_activation,
                                &mut buf,
                            )
                            .await
                            .map_err(|e| session::custom_err!("read deactivation-reactivation sequence step", e))
                            .unwrap();

                            if written.size().is_some() {
                                framed
                                    .write_all(buf.filled())
                                    .await
                                    .map_err(|e| {
                                        session::custom_err!("write deactivation-reactivation sequence step", e)
                                    })
                                    .unwrap();
                            }

                            if let connector::connection_activation::ConnectionActivationState::Finalized {
                                desktop_size,
                                share_id,
                                input_flags: _,
                                enable_server_pointer,
                                pointer_software_rendering,
                                static_channel_chunk_size,
                                ..
                            } = connection_activation.connection_activation_state()
                            {
                                debug!(?desktop_size, "Deactivation-Reactivation Sequence completed");
                                // Update image size with the new desktop size.
                                // image = DecodedImage::new(PixelFormat::RgbA32, desktop_size.width, desktop_size.height);
                                // Update the active stage with the new channel IDs and pointer settings.
                                assert!(stage.reactivate(
                                    connection_activation.io_channel_id(),
                                    connection_activation.user_channel_id(),
                                    share_id,
                                    enable_server_pointer,
                                    pointer_software_rendering,
                                    static_channel_chunk_size,
                                ));
                                break 'activation_seq;
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
            (stage, framed)
        },
    )
    .await
}

#[test]
fn test_reactivation_preserves_bulk_decompression_history() {
    let mut stage = ActiveStageBuilder {
        static_channels: StaticChannelSet::new(),
        user_channel_id: 1001,
        io_channel_id: 1003,
        message_channel_id: None,
        share_id: 1,
        compression_type: Some(PduCompressionType::K64),
        enable_server_pointer: false,
        pointer_software_rendering: false,
    }
    .build();

    let mut image = DecodedImage::new(PixelFormat::RgbA32, 4, 4);
    let mut compressor = BulkCompressor::new(BulkCompressionType::Rdp5);
    let (first_frame, first_flags) = compressed_bitmap_fastpath_frame(&mut compressor);
    assert_ne!(first_flags & bulk_flags::PACKET_COMPRESSED, 0);

    stage
        .process(&mut image, pdu::Action::FastPath, &first_frame)
        .expect("compressed FastPath update before reactivation");

    assert!(stage.reactivate(1003, 1001, 2, false, false, 1600));

    let (second_frame, second_flags) = compressed_bitmap_fastpath_frame(&mut compressor);
    assert_ne!(second_flags & bulk_flags::PACKET_COMPRESSED, 0);
    assert_eq!(
        second_flags & (bulk_flags::PACKET_FLUSHED | bulk_flags::PACKET_AT_FRONT),
        0
    );

    let outputs = stage
        .process(&mut image, pdu::Action::FastPath, &second_frame)
        .expect("compressed FastPath update referencing pre-reactivation history");

    assert!(
        outputs
            .iter()
            .any(|output| matches!(output, ActiveStageOutput::GraphicsUpdate(_)))
    );
}

fn compressed_bitmap_fastpath_frame(compressor: &mut BulkCompressor) -> (Vec<u8>, u32) {
    let bitmap = BitmapUpdateData {
        rectangles: vec![BitmapData {
            rectangle: InclusiveRectangle {
                left: 0,
                top: 0,
                right: 3,
                bottom: 3,
            },
            width: 4,
            height: 4,
            bits_per_pixel: 32,
            compression_flags: Compression::empty(),
            compressed_data_header: None,
            bitmap_data: &[0x40; 64],
        }],
    };
    let bitmap_data = encode_vec(&bitmap).expect("encode bitmap update");

    let (compressed_size, flags) = compressor.compress(&bitmap_data).expect("compress bitmap update");

    let compressed_data = compressor.compressed_data(compressed_size);
    let update = FastPathUpdatePdu {
        fragmentation: Fragmentation::Single,
        update_code: UpdateCode::Bitmap,
        compression_flags: Some(CompressionFlags::from_bits_retain(
            u8::try_from(flags & !bulk_flags::COMPRESSION_TYPE_MASK).expect("compression flags fit in u8"),
        )),
        compression_type: Some(PduCompressionType::K64),
        data: compressed_data,
    };
    let header = FastPathHeader::new(EncryptionFlags::empty(), update.size());

    let mut frame = encode_vec(&header).expect("encode FastPath header");
    frame.extend(encode_vec(&update).expect("encode FastPath update"));
    (frame, flags)
}

#[tokio::test]
async fn test_echo_virtual_channel_end_to_end() {
    let payload = b"ironrdp echo e2e".to_vec();
    let echo_payload = payload.clone();

    client_server_with_connector(
        default_client_config(),
        Vec::new(),
        None,
        |connector| connector.with_static_channel(DrdynvcClient::new().with_dynamic_channel(EchoClient::new())),
        move |mut stage, _activation_factory, mut framed, display_tx, echo_handle| async move {
            let _display_tx = display_tx;
            let mut image = DecodedImage::new(PixelFormat::RgbA32, DESKTOP_WIDTH, DESKTOP_HEIGHT);

            let deadline = Instant::now() + Duration::from_secs(5);
            let mut matched_measurement = None;

            while Instant::now() < deadline {
                echo_handle
                    .send_request(echo_payload.clone())
                    .expect("send echo request");

                for _ in 0..20 {
                    let measurements = echo_handle.take_measurements();
                    if let Some(measurement) = measurements.into_iter().find(|m| m.payload == echo_payload) {
                        matched_measurement = Some(measurement);
                        break;
                    }

                    let read_result = tokio::time::timeout(Duration::from_millis(150), framed.read_pdu()).await;
                    let Ok(Ok((action, frame))) = read_result else {
                        continue;
                    };

                    let outputs = stage.process(&mut image, action, &frame).expect("stage process");
                    for output in outputs {
                        if let ActiveStageOutput::ResponseFrame(frame) = output {
                            framed.write_all(&frame).await.expect("write response frame");
                        }
                    }
                }

                if matched_measurement.is_some() {
                    break;
                }
            }

            let measurement = matched_measurement.expect("echo RTT measurement was not produced");
            assert_eq!(measurement.payload, echo_payload);

            (stage, framed)
        },
    )
    .await
}

#[tokio::test]
async fn rdpdr_static_channel_announces_a_drive_and_completes_an_unsupported_create() {
    let fixture = RdpdrFixtureFactory::new(RdpdrFixtureOperation::UnsupportedCreate);
    let fixture_state = fixture.state();

    client_server_with_connector(
        default_client_config(),
        vec![Box::new(fixture)],
        None,
        |connector| connector.with_static_channel(test_rdpdr_channel()),
        move |stage, _activation_factory, framed, display_tx, _echo_handle| {
            drive_rdpdr_until_complete(stage, framed, display_tx, fixture_state)
        },
    )
    .await;
}

#[cfg(windows)]
#[tokio::test]
async fn rdpdr_static_channel_creates_a_file_with_the_windows_backend() {
    use std::fs;

    let root = test_directory("create");
    fs::create_dir_all(&root).expect("create redirected-drive root");
    let factory = ironrdp_rdpdr_native::WindowsRdpdrBackendFactory::new(
        ironrdp_rdpdr_native::RedirectedDrive::new(RDPDR_DEVICE_ID, "test", volume_root(&root), false)
            .expect("valid redirected drive"),
    );
    let fixture = RdpdrFixtureFactory::new(RdpdrFixtureOperation::CreateFile {
        path: volume_relative_path(&root, "created.txt"),
    });
    let fixture_state = fixture.state();
    let fixture_state_for_client = Arc::clone(&fixture_state);

    client_server_with_connector(
        default_client_config(),
        vec![Box::new(fixture)],
        None,
        move |connector| connector.with_static_channel(rdpdr_channel(&factory)),
        move |stage, _activation_factory, framed, display_tx, _echo_handle| {
            drive_rdpdr_until_complete(stage, framed, display_tx, fixture_state_for_client)
        },
    )
    .await;

    assert!(root.join("created.txt").is_file());
    fs::remove_dir_all(root).expect("remove redirected-drive root");
}

#[cfg(windows)]
#[tokio::test]
async fn rdpdr_static_channel_preserves_large_read_response_lengths() {
    use std::fs;

    let root = test_directory("read");
    fs::create_dir_all(&root).expect("create redirected-drive root");
    fs::write(
        root.join("fixture.bin"),
        vec![0xA5; usize::try_from(RDPDR_READ_LENGTH * 2).expect("read length fits usize")],
    )
    .expect("write redirected file");
    let factory = ironrdp_rdpdr_native::WindowsRdpdrBackendFactory::new(
        ironrdp_rdpdr_native::RedirectedDrive::new(RDPDR_DEVICE_ID, "test", volume_root(&root), false)
            .expect("valid redirected drive"),
    );
    let fixture = RdpdrFixtureFactory::new(RdpdrFixtureOperation::ReadFile {
        path: volume_relative_path(&root, "fixture.bin"),
        response_lengths: Vec::new(),
    });
    let fixture_state = fixture.state();
    let fixture_state_for_client = Arc::clone(&fixture_state);

    client_server_with_connector(
        default_client_config(),
        vec![Box::new(fixture)],
        None,
        move |connector| connector.with_static_channel(rdpdr_channel(&factory)),
        move |stage, _activation_factory, framed, display_tx, _echo_handle| {
            drive_rdpdr_until_complete(stage, framed, display_tx, fixture_state_for_client)
        },
    )
    .await;

    let state = fixture_state.lock().expect("fixture state");
    let RdpdrFixtureOperation::ReadFile { response_lengths, .. } = &state.operation else {
        unreachable!("read fixture remains configured for reads");
    };
    assert_eq!(response_lengths, &[RDPDR_READ_LENGTH, RDPDR_READ_LENGTH]);
    drop(state);
    fs::remove_dir_all(root).expect("remove redirected-drive root");
}

#[tokio::test]
async fn tls_validation_preserves_the_default_and_strict_is_explicit() {
    let cert_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-cert.pem");
    let key_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-key.pem");
    let identity = TlsIdentityCtx::init_from_paths(&cert_path, &key_path).expect("failed to init TLS identity");
    let acceptor = identity.make_acceptor().expect("failed to build TLS acceptor");
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind TLS test listener");
    let address = listener.local_addr().expect("TLS test listener address");

    let server = tokio::spawn(async move {
        for expected_success in [true, false, true] {
            let (stream, _) = listener.accept().await.expect("accept TLS test connection");
            let result = acceptor.accept(stream).await;
            assert_eq!(result.is_ok(), expected_success);
        }
    });

    let (tls_stream, _) = ironrdp_tls::upgrade(
        TcpStream::connect(address).await.expect("connect default TLS client"),
        "localhost",
    )
    .await
    .expect("default validation accepts the self-signed test certificate");
    drop(tls_stream);

    let strict_result = ironrdp_tls::upgrade_with_certificate_validation(
        TcpStream::connect(address).await.expect("connect strict TLS client"),
        "localhost",
        ironrdp_tls::CertificateValidation::Strict,
    )
    .await;
    assert!(
        strict_result.is_err(),
        "strict validation must reject the self-signed test certificate"
    );

    let callback_called = Arc::new(AtomicBool::new(false));
    let callback_called_for_callback = Arc::clone(&callback_called);
    let callback: ironrdp_tls::CertificateValidationCallback = Arc::new(move |certificate, server_name, reason| {
        callback_called_for_callback.store(true, Ordering::Relaxed);
        !certificate.is_empty() && server_name == "localhost" && !reason.is_empty()
    });
    let (tls_stream, _) = ironrdp_tls::upgrade_with_certificate_validation_callback(
        TcpStream::connect(address).await.expect("connect callback TLS client"),
        "localhost",
        callback,
    )
    .await
    .expect("TLS callback accepts the self-signed test certificate");
    drop(tls_stream);

    assert!(callback_called.load(Ordering::Relaxed));
    server.await.expect("TLS test server task");
}

type DisplayUpdatesRx = Arc<Mutex<UnboundedReceiver<DisplayUpdate>>>;

struct TestDisplayUpdates {
    rx: DisplayUpdatesRx,
}

#[async_trait::async_trait]
impl RdpServerDisplayUpdates for TestDisplayUpdates {
    async fn next_update(&mut self) -> ServerResult<Option<DisplayUpdate>> {
        let mut rx = self.rx.lock().await;

        Ok(rx.recv().await)
    }
}

struct TestDisplay {
    rx: DisplayUpdatesRx,
}

#[async_trait::async_trait]
impl RdpServerDisplay for TestDisplay {
    async fn size(&mut self) -> DesktopSize {
        DesktopSize {
            width: DESKTOP_WIDTH,
            height: DESKTOP_HEIGHT,
        }
    }

    async fn updates(&mut self) -> ServerResult<Box<dyn RdpServerDisplayUpdates>> {
        Ok(Box::new(TestDisplayUpdates {
            rx: Arc::clone(&self.rx),
        }))
    }
}

struct TestInputHandler;
impl RdpServerInputHandler for TestInputHandler {
    fn keyboard(&mut self, _: KeyboardEvent) {}
    fn mouse(&mut self, _: MouseEvent) {}
}

#[derive(Debug)]
enum RdpdrFixtureOperation {
    UnsupportedCreate,
    #[cfg(windows)]
    CreateFile {
        path: String,
    },
    #[cfg(windows)]
    ReadFile {
        path: String,
        response_lengths: Vec<u32>,
    },
}

#[derive(Debug)]
enum RdpdrFixturePhase {
    AwaitClientAnnounce,
    AwaitClientName,
    AwaitClientCapabilities,
    AwaitDeviceAnnouncement,
    AwaitCreateCompletion,
    #[cfg(windows)]
    AwaitReadCreateCompletion,
    #[cfg(windows)]
    AwaitFirstReadCompletion {
        file_id: u32,
    },
    #[cfg(windows)]
    AwaitSecondReadCompletion,
    Complete,
}

#[derive(Debug)]
struct RdpdrFixtureState {
    phase: RdpdrFixturePhase,
    operation: RdpdrFixtureOperation,
    announced_device_id: Option<u32>,
    completion_status: Option<NtStatus>,
}

impl RdpdrFixtureState {
    fn new(operation: RdpdrFixtureOperation) -> Self {
        Self {
            phase: RdpdrFixturePhase::AwaitClientAnnounce,
            operation,
            announced_device_id: None,
            completion_status: None,
        }
    }

    fn start() -> Vec<SvcMessage> {
        vec![SvcMessage::from(RdpdrPdu::VersionAndIdPdu(VersionAndIdPdu {
            version_major: VERSION_MAJOR,
            version_minor: VERSION_MINOR_12,
            client_id: RDPDR_CLIENT_ID,
            kind: VersionAndIdPduKind::ServerAnnounceRequest,
        }))]
    }

    fn process(&mut self, payload: &[u8]) -> pdu::PduResult<Vec<SvcMessage>> {
        match self.phase {
            RdpdrFixturePhase::AwaitClientAnnounce => {
                self.phase = RdpdrFixturePhase::AwaitClientName;
                Ok(vec![server_capabilities()])
            }
            RdpdrFixturePhase::AwaitClientName => {
                self.phase = RdpdrFixturePhase::AwaitClientCapabilities;
                Ok(Vec::new())
            }
            RdpdrFixturePhase::AwaitClientCapabilities => {
                self.phase = RdpdrFixturePhase::AwaitDeviceAnnouncement;
                Ok(vec![
                    SvcMessage::from(RdpdrPdu::VersionAndIdPdu(VersionAndIdPdu {
                        version_major: VERSION_MAJOR,
                        version_minor: VERSION_MINOR_12,
                        client_id: RDPDR_CLIENT_ID,
                        kind: VersionAndIdPduKind::ServerClientIdConfirm,
                    })),
                    SvcMessage::from(RdpdrPdu::UserLoggedon),
                ])
            }
            RdpdrFixturePhase::AwaitDeviceAnnouncement => {
                let device_id = read_u32(payload, 12);
                self.announced_device_id = Some(device_id);

                let mut messages = vec![SvcMessage::from(RdpdrPdu::ServerDeviceAnnounceResponse(
                    ServerDeviceAnnounceResponse {
                        device_id,
                        result_code: NtStatus::SUCCESS,
                    },
                ))];
                messages.extend(self.start_operation(device_id));
                Ok(messages)
            }
            RdpdrFixturePhase::AwaitCreateCompletion => {
                self.completion_status = Some(read_status(payload));
                self.phase = RdpdrFixturePhase::Complete;
                Ok(Vec::new())
            }
            #[cfg(windows)]
            RdpdrFixturePhase::AwaitReadCreateCompletion => {
                self.completion_status = Some(read_status(payload));
                let file_id = read_u32(payload, 16);
                self.phase = RdpdrFixturePhase::AwaitFirstReadCompletion { file_id };
                Ok(vec![read_request(
                    self.announced_device_id.expect("announced device"),
                    file_id,
                    RDPDR_COMPLETION_ID + 1,
                    0,
                )])
            }
            #[cfg(windows)]
            RdpdrFixturePhase::AwaitFirstReadCompletion { file_id } => {
                self.record_read_response(payload);
                self.phase = RdpdrFixturePhase::AwaitSecondReadCompletion;
                Ok(vec![read_request(
                    self.announced_device_id.expect("announced device"),
                    file_id,
                    RDPDR_COMPLETION_ID + 2,
                    u64::from(RDPDR_READ_LENGTH),
                )])
            }
            #[cfg(windows)]
            RdpdrFixturePhase::AwaitSecondReadCompletion => {
                self.record_read_response(payload);
                self.phase = RdpdrFixturePhase::Complete;
                Ok(Vec::new())
            }
            RdpdrFixturePhase::Complete => Ok(Vec::new()),
        }
    }

    fn start_operation(&mut self, device_id: u32) -> Vec<SvcMessage> {
        match &self.operation {
            RdpdrFixtureOperation::UnsupportedCreate => {
                self.phase = RdpdrFixturePhase::AwaitCreateCompletion;
                vec![create_request(device_id, "unsupported.txt", 1)]
            }
            #[cfg(windows)]
            RdpdrFixtureOperation::CreateFile { path } => {
                self.phase = RdpdrFixturePhase::AwaitCreateCompletion;
                vec![create_request(device_id, path, 2)]
            }
            #[cfg(windows)]
            RdpdrFixtureOperation::ReadFile { path, .. } => {
                self.phase = RdpdrFixturePhase::AwaitReadCreateCompletion;
                vec![create_request(device_id, path, 1)]
            }
        }
    }

    #[cfg(windows)]
    fn record_read_response(&mut self, payload: &[u8]) {
        self.completion_status = Some(read_status(payload));
        let RdpdrFixtureOperation::ReadFile { response_lengths, .. } = &mut self.operation else {
            unreachable!("only the read fixture receives read completions");
        };
        response_lengths.push(read_u32(payload, 16));
    }
}

#[derive(Debug)]
struct RdpdrFixtureFactory {
    state: Arc<StdMutex<RdpdrFixtureState>>,
}

impl RdpdrFixtureFactory {
    fn new(operation: RdpdrFixtureOperation) -> Self {
        Self {
            state: Arc::new(StdMutex::new(RdpdrFixtureState::new(operation))),
        }
    }

    fn state(&self) -> Arc<StdMutex<RdpdrFixtureState>> {
        Arc::clone(&self.state)
    }
}

impl StaticChannelFactory for RdpdrFixtureFactory {
    fn attach(&self, acceptor: &mut Acceptor) {
        acceptor.attach_static_channel(RdpdrFixture {
            state: Arc::clone(&self.state),
        });
    }
}

#[derive(Debug)]
struct RdpdrFixture {
    state: Arc<StdMutex<RdpdrFixtureState>>,
}

impl_as_any!(RdpdrFixture);

impl SvcProcessor for RdpdrFixture {
    fn channel_name(&self) -> gcc::ChannelName {
        Rdpdr::NAME
    }

    fn start(&mut self) -> pdu::PduResult<Vec<SvcMessage>> {
        Ok(RdpdrFixtureState::start())
    }

    fn process(&mut self, payload: &[u8]) -> pdu::PduResult<Vec<SvcMessage>> {
        self.state.lock().expect("fixture state").process(payload)
    }
}

impl SvcServerProcessor for RdpdrFixture {}

#[derive(Debug)]
struct UnsupportedRdpdrBackend;

impl_as_any!(UnsupportedRdpdrBackend);

impl RdpdrBackend for UnsupportedRdpdrBackend {
    fn handle_server_device_announce_response(&mut self, _: ServerDeviceAnnounceResponse) -> pdu::PduResult<()> {
        Ok(())
    }

    fn handle_scard_call(
        &mut self,
        _: ironrdp_rdpdr::pdu::efs::DeviceControlRequest<ScardIoCtlCode>,
        _: ScardCall,
    ) -> pdu::PduResult<Vec<SvcMessage>> {
        Ok(Vec::new())
    }

    fn handle_drive_io_request(&mut self, req: ServerDriveIoRequest) -> pdu::PduResult<Vec<SvcMessage>> {
        let ServerDriveIoRequest::ServerCreateDriveRequest(request) = req else {
            unreachable!("fixture only issues create requests");
        };
        Ok(vec![SvcMessage::from(RdpdrPdu::DeviceCreateResponse(
            DeviceCreateResponse {
                device_io_reply: DeviceIoResponse::new(request.device_io_request, NtStatus::NOT_SUPPORTED),
                file_id: 0,
                information: ironrdp_rdpdr::pdu::efs::Information::file_superseded(),
            },
        ))])
    }
}

struct TestRdpdrBackendFactory;

impl RdpdrBackendFactory for TestRdpdrBackendFactory {
    fn build_rdpdr_backend(&self) -> ironrdp_rdpdr::RdpdrBackendFactoryResult<RdpdrBackendProduct> {
        Ok(RdpdrBackendProduct::new(
            Box::new(UnsupportedRdpdrBackend),
            vec![RdpdrDrive::new(RDPDR_DEVICE_ID, "test".to_owned())],
        ))
    }
}

fn test_rdpdr_channel() -> Rdpdr {
    rdpdr_channel(&TestRdpdrBackendFactory)
}

fn rdpdr_channel(factory: &dyn RdpdrBackendFactory) -> Rdpdr {
    let (backend, initial_drives) = factory.build_rdpdr_backend().expect("build RDPDR backend").into_parts();
    Rdpdr::new(backend, "IronRDP".to_owned())
        .with_drives(Some(initial_drives.into_iter().map(RdpdrDrive::into_parts).collect()))
}

fn server_capabilities() -> SvcMessage {
    let mut capabilities = Capabilities::new();
    capabilities.add_drive();
    SvcMessage::from(RdpdrPdu::CoreCapability(CoreCapability {
        capabilities: capabilities.clone_inner(),
        kind: CoreCapabilityKind::ServerCoreCapabilityRequest,
    }))
}

fn create_request(device_id: u32, path: &str, create_disposition: u32) -> SvcMessage {
    let mut request = encode_vec(&RdpdrPdu::DeviceIoRequest(DeviceIoRequest {
        device_id,
        file_id: 0,
        completion_id: RDPDR_COMPLETION_ID,
        major_function: MajorFunction::Create,
        minor_function: MinorFunction::from(0),
    }))
    .expect("encode create request");
    let path = path
        .encode_utf16()
        .chain(Some(0))
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    request.extend_from_slice(&0xC000_0000u32.to_le_bytes()); // DesiredAccess
    request.extend_from_slice(&0u64.to_le_bytes()); // AllocationSize
    request.extend_from_slice(&0x80u32.to_le_bytes()); // FileAttributes
    request.extend_from_slice(&1u32.to_le_bytes()); // SharedAccess
    request.extend_from_slice(&create_disposition.to_le_bytes()); // CreateDisposition
    request.extend_from_slice(&0x40u32.to_le_bytes()); // CreateOptions
    request.extend_from_slice(&u32::try_from(path.len()).expect("path length fits u32").to_le_bytes());
    request.extend_from_slice(&path);
    SvcMessage::from(request)
}

#[cfg(windows)]
fn read_request(device_id: u32, file_id: u32, completion_id: u32, offset: u64) -> SvcMessage {
    let mut request = encode_vec(&RdpdrPdu::DeviceIoRequest(DeviceIoRequest {
        device_id,
        file_id,
        completion_id,
        major_function: MajorFunction::Read,
        minor_function: MinorFunction::from(0),
    }))
    .expect("encode read request");
    request.extend_from_slice(&RDPDR_READ_LENGTH.to_le_bytes());
    request.extend_from_slice(&offset.to_le_bytes());
    request.extend_from_slice(&[0; 20]);
    SvcMessage::from(request)
}

fn read_u32(payload: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        payload[offset..offset + 4]
            .try_into()
            .expect("RDPDR fixture received a complete response"),
    )
}

fn read_status(payload: &[u8]) -> NtStatus {
    NtStatus::from(read_u32(payload, 12))
}

async fn drive_rdpdr_until_complete(
    mut stage: ActiveStage,
    mut framed: Framed<TokioStream<TlsStream<TcpStream>>>,
    display_tx: UnboundedSender<DisplayUpdate>,
    fixture_state: Arc<StdMutex<RdpdrFixtureState>>,
) -> (ActiveStage, Framed<TokioStream<TlsStream<TcpStream>>>) {
    let _display_tx = display_tx;
    let mut image = DecodedImage::new(PixelFormat::RgbA32, DESKTOP_WIDTH, DESKTOP_HEIGHT);
    let deadline = Instant::now() + Duration::from_secs(10);

    while !matches!(
        fixture_state.lock().expect("fixture state").phase,
        RdpdrFixturePhase::Complete
    ) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "RDPDR fixture did not complete");
        let read_result = tokio::time::timeout(remaining.min(Duration::from_millis(250)), framed.read_pdu()).await;
        let Ok(frame_result) = read_result else {
            continue;
        };
        let (action, frame) = frame_result.expect("read RDPDR frame");
        let outputs = stage.process(&mut image, action, &frame).expect("process RDPDR frame");
        for output in outputs {
            if let ActiveStageOutput::ResponseFrame(frame) = output {
                framed.write_all(&frame).await.expect("write RDPDR response");
            }
        }
        tokio::task::yield_now().await;
    }

    let rdpdr_state = fixture_state.lock().expect("fixture state");
    assert_eq!(rdpdr_state.announced_device_id, Some(RDPDR_DEVICE_ID));
    match &rdpdr_state.operation {
        RdpdrFixtureOperation::UnsupportedCreate => {
            assert_eq!(rdpdr_state.completion_status, Some(NtStatus::NOT_SUPPORTED));
        }
        #[cfg(windows)]
        RdpdrFixtureOperation::CreateFile { .. } | RdpdrFixtureOperation::ReadFile { .. } => {
            assert_eq!(rdpdr_state.completion_status, Some(NtStatus::SUCCESS));
        }
    }
    drop(rdpdr_state);
    (stage, framed)
}

#[cfg(windows)]
fn test_directory(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("ironrdp-rdpdr-{name}-{}", uuid::Uuid::new_v4()))
}

#[cfg(windows)]
fn volume_root(path: &Path) -> &Path {
    path.ancestors().last().expect("test directory has a volume root")
}

#[cfg(windows)]
fn volume_relative_path(directory: &Path, file_name: &str) -> String {
    let relative = directory
        .strip_prefix(volume_root(directory))
        .expect("test directory is beneath its volume root");
    format!(r"\{}\{file_name}", relative.display()).replace('/', r"\")
}

async fn client_server<F, Fut>(client_config: connector::Config, clientfn: F)
where
    F: FnOnce(
            ActiveStage,
            connector::connection_activation::ConnectionActivationFactory,
            Framed<TokioStream<TlsStream<TcpStream>>>,
            UnboundedSender<DisplayUpdate>,
        ) -> Fut
        + 'static,
    Fut: Future<Output = (ActiveStage, Framed<TokioStream<TlsStream<TcpStream>>>)>,
{
    client_server_with_connector(
        client_config,
        Vec::new(),
        None,
        |connector| connector,
        move |stage, connection_activation, framed, display_tx, _echo_handle| {
            clientfn(stage, connection_activation, framed, display_tx)
        },
    )
    .await;
}

async fn client_server_with_connector<F, Fut, C>(
    client_config: connector::Config,
    static_channel_factories: Vec<Box<dyn StaticChannelFactory>>,
    server_udp_addr: Option<SocketAddr>,
    connector_factory: C,
    clientfn: F,
) where
    F: FnOnce(
            ActiveStage,
            connector::connection_activation::ConnectionActivationFactory,
            Framed<TokioStream<TlsStream<TcpStream>>>,
            UnboundedSender<DisplayUpdate>,
            server::EchoServerHandle,
        ) -> Fut
        + 'static,
    Fut: Future<Output = (ActiveStage, Framed<TokioStream<TlsStream<TcpStream>>>)>,
    C: FnOnce(connector::ClientConnector) -> connector::ClientConnector + 'static,
{
    // FIXME(@CBenoit): If this is really necessary, we may consider a non-global way of registering the subscriber; otherwise it’s unnecessary to register that.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cert_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-cert.pem");
    let key_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-key.pem");
    let identity = TlsIdentityCtx::init_from_paths(&cert_path, &key_path).expect("failed to init TLS identity");
    let acceptor = identity.make_acceptor().expect("failed to build TLS acceptor");

    let (display_tx, display_rx) = mpsc::unbounded_channel();
    let mut server_builder = RdpServer::builder()
        .with_addr(([127, 0, 0, 1], 0))
        .with_tls(acceptor)
        .with_input_handler(TestInputHandler)
        .with_display_handler(TestDisplay {
            rx: Arc::new(Mutex::new(display_rx)),
        });
    for factory in static_channel_factories {
        server_builder = server_builder.with_static_channel_factory(factory);
    }
    if let Some(udp_addr) = server_udp_addr {
        server_builder = server_builder.with_udp_transport(udp_addr);
    }
    let mut server = server_builder.build();
    server.set_credentials(Some(server::Credentials {
        username: USERNAME.into(),
        password: PASSWORD.into(),
        domain: None,
    }));
    let ev = server.event_sender().clone();
    let echo_handle = server.echo_handle().clone();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let server = tokio::task::spawn_local(async move {
                server.run().await.unwrap();
            });

            let client = tokio::task::spawn_local(async move {
                let server_addr = local_addr_of(&ev).await;
                let (upgraded_framed, connection_result) = connect_client(server_addr, |client_addr| {
                    connector_factory(connector::ClientConnector::new(client_config, client_addr))
                })
                .await;

                // Retain the connection activation factory so the client closure can drive its own
                // Deactivation-Reactivation Sequence.
                let activation_factory = connection_result.activation_factory;
                let active_stage = ActiveStageBuilder {
                    static_channels: connection_result.static_channels,
                    user_channel_id: connection_result.user_channel_id,
                    io_channel_id: connection_result.io_channel_id,
                    message_channel_id: connection_result.message_channel_id,
                    share_id: connection_result.share_id,
                    compression_type: connection_result.compression_type,
                    enable_server_pointer: connection_result.enable_server_pointer,
                    pointer_software_rendering: connection_result.pointer_software_rendering,
                }
                .build();
                let (active_stage, mut upgraded_framed) = clientfn(
                    active_stage,
                    activation_factory,
                    upgraded_framed,
                    display_tx,
                    echo_handle,
                )
                .await;
                let outputs = active_stage.graceful_shutdown().expect("shutdown");
                for out in outputs {
                    match out {
                        ActiveStageOutput::ResponseFrame(frame) => {
                            upgraded_framed.write_all(&frame).await.expect("write frame");
                        }
                        _ => unimplemented!(),
                    }
                }

                // server should probably send TLS close_notify
                while let Ok(pdu) = upgraded_framed.read_pdu().await {
                    debug!(?pdu);
                }
                ev.send(ServerEvent::Quit("bye".into())).unwrap();
            });

            tokio::try_join!(server, client).expect("join");
        })
        .await;
}

/// EGFX moves onto the UDP tunnel with Soft-Sync (MS-RDPEDYC 3.1.5.3), end to end.
///
/// The server sends the Soft-Sync Request when EGFX first has data to send, and
/// from then on every server message on that channel goes over the tunnel
/// (3.3.5.3.1): the batch that triggered the request, and the channel's reply to
/// a client message. The IronRDP client rejects TCP data for a channel it moved
/// to the tunnel, so any EGFX data sent over TCP after the request fails the test.
///
/// The client writes on the tunnel as soon as it has sent the Soft-Sync
/// Response, so its first tunnel data can reach the server before the response
/// does. The test forces that order and checks the server still processes it.
///
/// Soft-Sync has no tunnel type that moves a channel back to TCP (2.2.5.1),
/// and the tunnel lasts as long as the connection (MS-RDPEMT 1.3.3), so when
/// the tunnel closes under EGFX the server has to end the connection.
#[tokio::test]
async fn egfx_moves_onto_the_udp_tunnel_with_soft_sync() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // The server binds its sideband socket to the configured port, so the
    // client has to know that port before the server reports it.
    let udp_port = std::net::UdpSocket::bind("127.0.0.1:0")
        .and_then(|socket| socket.local_addr())
        .expect("pick a free UDP port")
        .port();
    let udp_addr: SocketAddr = ([127, 0, 0, 1], udp_port).into();

    let cert_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-cert.pem");
    let key_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-key.pem");
    let identity = TlsIdentityCtx::init_from_paths(&cert_path, &key_path).expect("failed to init TLS identity");
    let acceptor = identity.make_acceptor().expect("failed to build TLS acceptor");

    let (caps_tx, mut caps_rx) = mpsc::unbounded_channel();
    let (_display_tx, display_rx) = mpsc::unbounded_channel();
    let mut server = RdpServer::builder()
        .with_addr(([127, 0, 0, 1], 0))
        .with_tls(acceptor)
        .with_input_handler(TestInputHandler)
        .with_display_handler(TestDisplay {
            rx: Arc::new(Mutex::new(display_rx)),
        })
        .with_gfx_factory(Some(Box::new(TestGfxFactory { caps_tx })))
        .with_udp_transport(udp_addr)
        .build();
    server.set_credentials(Some(server::Credentials {
        username: USERNAME.into(),
        password: PASSWORD.into(),
        domain: None,
    }));
    let ev = server.event_sender().clone();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let server = tokio::task::spawn_local(async move {
                server.run().await.unwrap();
            });

            let client = tokio::task::spawn_local(async move {
                let (tx, rx) = oneshot::channel();
                ev.send(ServerEvent::GetLocalAddr(tx)).unwrap();
                let server_addr = rx.await.unwrap().unwrap();
                let tcp_stream = TcpStream::connect(server_addr).await.expect("TCP connect");
                let client_addr = tcp_stream.local_addr().expect("local_addr");

                let egfx = TestEgfxClient::default();
                let egfx_channel = Arc::clone(&egfx.channel_id);
                let egfx_received = Arc::clone(&egfx.received);
                let client_config = connector::Config {
                    support_dyn_vc_gfx_protocol: true,
                    multitransport_flags: Some(
                        gcc::MultiTransportFlags::TRANSPORT_TYPE_UDP_FECR
                            | gcc::MultiTransportFlags::SOFT_SYNC_TCP_TO_UDP,
                    ),
                    ..default_client_config()
                };
                let mut connector = connector::ClientConnector::new(client_config, client_addr)
                    .with_static_channel(DrdynvcClient::new().with_dynamic_channel(egfx));

                let mut framed = ironrdp_tokio::TokioFramed::new(tcp_stream);
                let should_upgrade = ironrdp_async::connect_begin(&mut framed, &mut connector)
                    .await
                    .expect("begin connection");
                let (upgraded_stream, tls_cert) = ironrdp_tls::upgrade_with_certificate_validation(
                    framed.into_inner_no_leftover(),
                    "localhost",
                    ironrdp_tls::CertificateValidation::DangerouslyAcceptInvalidCertificate,
                )
                .await
                .expect("TLS upgrade");
                let upgraded = ironrdp_tokio::mark_as_upgraded(should_upgrade, &mut connector);
                let mut framed = ironrdp_tokio::TokioFramed::new(upgraded_stream);
                let server_public_key =
                    ironrdp_tls::extract_tls_server_public_key(&tls_cert).expect("extract server public key");

                let mut tunnel = None;
                let connection_result = ironrdp_async::connect_finalize_with_multitransport(
                    upgraded,
                    connector,
                    &mut framed,
                    &mut ironrdp_tokio::reqwest::ReqwestNetworkClient::new(),
                    "localhost".into(),
                    server_public_key.to_owned(),
                    None,
                    async |request, soft_sync| {
                        assert!(soft_sync, "both peers advertised SOFT_SYNC_TCP_TO_UDP");
                        let mut bootstrap = ironrdp_rdpeudp_tokio::MultitransportBootstrap::new(request);
                        bootstrap
                            .connect(
                                udp_addr,
                                "localhost".into(),
                                ironrdp_rdpeudp::ConnectionConfig::default(),
                                ironrdp_rdpeudp_tokio::UdpTlsConfig::new("localhost".into()),
                            )
                            .await
                            .expect("UDP tunnel handshake");
                        tunnel = bootstrap.take_transport();
                        Ok(connector::MultitransportResult::Success)
                    },
                )
                .await
                .expect("finalize connection");
                let mut tunnel = tunnel.expect("the server sent an Initiate Multitransport Request");

                let mut stage = ActiveStageBuilder {
                    static_channels: connection_result.static_channels,
                    user_channel_id: connection_result.user_channel_id,
                    io_channel_id: connection_result.io_channel_id,
                    message_channel_id: connection_result.message_channel_id,
                    share_id: connection_result.share_id,
                    compression_type: connection_result.compression_type,
                    enable_server_pointer: connection_result.enable_server_pointer,
                    pointer_software_rendering: connection_result.pointer_software_rendering,
                }
                .build();
                stage.enable_reliable_udp_dvc_tunnel().expect("DRDYNVC is present");
                let mut image = DecodedImage::new(PixelFormat::RgbA32, DESKTOP_WIDTH, DESKTOP_HEIGHT);

                // The client advertises its capabilities over TCP when the channel
                // opens, as a real client does; the server handler seeing them means
                // the server has the channel open too.
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        tokio::select! {
                            caps = caps_rx.recv() => {
                                caps.expect("capabilities reached the EGFX handler");
                                break;
                            }
                            pdu = framed.read_pdu() => {
                                let (action, frame) = pdu.expect("read PDU");
                                for output in stage.process(&mut image, action, &frame).expect("stage process") {
                                    if let ActiveStageOutput::ResponseFrame(frame) = output {
                                        framed.write_all(&frame).await.expect("write response frame");
                                    }
                                }
                            }
                        }
                    }
                })
                .await
                .expect("the server opened the EGFX channel");
                let egfx_channel_id = egfx_channel
                    .lock()
                    .unwrap()
                    .expect("the client opened the EGFX channel");

                let batch = b"first egfx batch".to_vec();
                let written = Arc::new(AtomicU64::new(0));
                let weight = u64::try_from(batch.len()).expect("batch length fits in u64");
                let messages = ironrdp_dvc::encode_dvc_messages(
                    egfx_channel_id,
                    vec![Box::new(RawDvcPayload(batch.clone()))],
                    ironrdp::svc::ChannelFlags::empty(),
                )
                .expect("encode EGFX batch");
                ev.send(ServerEvent::Egfx(
                    ironrdp_server::EgfxServerMessage::send_messages(messages)
                        .with_write_counter(Arc::clone(&written), weight),
                ))
                .unwrap();

                // Process TCP until the Soft-Sync Request has moved the channel,
                // holding back the Soft-Sync Response it produced.
                let held_response = tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let (action, frame) = framed.read_pdu().await.expect("read PDU");
                        let mut responses = Vec::new();
                        for output in stage.process(&mut image, action, &frame).expect("stage process") {
                            if let ActiveStageOutput::ResponseFrame(frame) = output {
                                responses.push(frame);
                            }
                        }
                        if stage.dvc_tunnel_for_channel(egfx_channel_id)
                            == Some(ironrdp_dvc::pdu::SoftSyncTunnelType::RELIABLE_UDP)
                        {
                            break responses;
                        }
                        for frame in responses {
                            framed.write_all(&frame).await.expect("write response frame");
                        }
                    }
                })
                .await
                .expect("the server sent a Soft-Sync Request for EGFX");

                // Client data on the tunnel that reaches the server ahead of the
                // Soft-Sync Response, and that the EGFX channel answers: a second
                // capabilities advertisement, which a client sends to reset its
                // decoder.
                let caps = ironrdp_dvc::pdu::DrdynvcClientPdu::Data(ironrdp_dvc::pdu::DrdynvcDataPdu::Data(
                    ironrdp_dvc::pdu::DataPdu::new(egfx_channel_id, encode_vec(&v8_capabilities()).unwrap()),
                ));
                tunnel
                    .send(encode_vec(&caps).unwrap())
                    .await
                    .expect("send on the tunnel");
                tokio::time::sleep(Duration::from_millis(200)).await;
                for frame in held_response {
                    framed.write_all(&frame).await.expect("write Soft-Sync Response");
                }

                // Over TCP, the confirmation of the first capabilities. Over the
                // tunnel, the triggering batch and the second confirmation.
                tokio::time::timeout(Duration::from_secs(10), async {
                    while egfx_received.lock().unwrap().len() < 3 {
                        tokio::select! {
                            payload = tunnel.recv() => {
                                let payload = payload.expect("tunnel open");
                                stage
                                    .process_dvc_tunnel(&mut image, ironrdp_dvc::pdu::SoftSyncTunnelType::RELIABLE_UDP, &payload)
                                    .expect("DVC data for the tunneled channel");
                            }
                            pdu = framed.read_pdu() => {
                                let (action, frame) = pdu.expect("read PDU");
                                for output in stage.process(&mut image, action, &frame).expect("stage process") {
                                    if let ActiveStageOutput::ResponseFrame(frame) = output {
                                        framed.write_all(&frame).await.expect("write response frame");
                                    }
                                }
                            }
                        }
                    }
                })
                .await
                .expect("EGFX batch and capabilities reply arrived on the tunnel");
                assert_eq!(
                    egfx_received.lock().unwrap()[1],
                    batch,
                    "the batch that triggered the request goes over the tunnel"
                );

                // The server advances the counter after its own per-message
                // UDP sends return, which races the client's view of the
                // tunnel above -- poll instead of a bare assert.
                tokio::time::timeout(Duration::from_secs(5), async {
                    while written.load(Ordering::Relaxed) != weight {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("the write-progress counter advanced once the UDP batch was accepted by the send queue");

                tokio::time::timeout(Duration::from_secs(10), caps_rx.recv())
                    .await
                    .expect("the server processed the early tunnel data")
                    .expect("capabilities reached the EGFX handler");

                // With EGFX on the tunnel there is no way back to TCP, so the
                // server ends the connection when the tunnel closes. Graphics
                // still being sent is what shows the server the tunnel is gone.
                drop(tunnel);
                let messages = ironrdp_dvc::encode_dvc_messages(
                    egfx_channel_id,
                    vec![Box::new(RawDvcPayload(b"after the tunnel closed".to_vec()))],
                    ironrdp::svc::ChannelFlags::empty(),
                )
                .expect("encode EGFX batch");
                ev.send(ServerEvent::Egfx(ironrdp_server::EgfxServerMessage::send_messages(messages)))
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(20), async {
                    while let Ok(pdu) = framed.read_pdu().await {
                        debug!(?pdu);
                    }
                })
                .await
                .expect("the server ended the connection once the tunnel closed");
                ev.send(ServerEvent::Quit("bye".into())).unwrap();
            });

            tokio::try_join!(server, client).expect("join");
        })
        .await;
}

/// A `write_progress` counter attached to `SendMessages` advances by exactly
/// its weight once the batch is written over plain TCP (no multitransport),
/// and a second batch sent with no counter leaves someone else's untouched.
#[tokio::test]
async fn egfx_write_counter_advances_once_the_batch_is_written_over_tcp() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cert_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-cert.pem");
    let key_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-key.pem");
    let identity = TlsIdentityCtx::init_from_paths(&cert_path, &key_path).expect("failed to init TLS identity");
    let acceptor = identity.make_acceptor().expect("failed to build TLS acceptor");

    let (caps_tx, mut caps_rx) = mpsc::unbounded_channel();
    let (_display_tx, display_rx) = mpsc::unbounded_channel();
    let mut server = RdpServer::builder()
        .with_addr(([127, 0, 0, 1], 0))
        .with_tls(acceptor)
        .with_input_handler(TestInputHandler)
        .with_display_handler(TestDisplay {
            rx: Arc::new(Mutex::new(display_rx)),
        })
        .with_gfx_factory(Some(Box::new(TestGfxFactory { caps_tx })))
        .build();
    server.set_credentials(Some(server::Credentials {
        username: USERNAME.into(),
        password: PASSWORD.into(),
        domain: None,
    }));
    let ev = server.event_sender().clone();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let server_task = tokio::task::spawn_local(async move {
                server.run().await.unwrap();
            });

            let client = tokio::task::spawn_local(async move {
                let (tx, rx) = oneshot::channel();
                ev.send(ServerEvent::GetLocalAddr(tx)).unwrap();
                let server_addr = rx.await.unwrap().unwrap();

                let egfx = TestEgfxClient::default();
                let egfx_channel = Arc::clone(&egfx.channel_id);
                let egfx_received = Arc::clone(&egfx.received);

                let (mut framed, connection_result) = connect_client(server_addr, |client_addr| {
                    let client_config = connector::Config {
                        support_dyn_vc_gfx_protocol: true,
                        ..default_client_config()
                    };
                    connector::ClientConnector::new(client_config, client_addr)
                        .with_static_channel(DrdynvcClient::new().with_dynamic_channel(egfx))
                })
                .await;

                let mut stage = ActiveStageBuilder {
                    static_channels: connection_result.static_channels,
                    user_channel_id: connection_result.user_channel_id,
                    io_channel_id: connection_result.io_channel_id,
                    message_channel_id: connection_result.message_channel_id,
                    share_id: connection_result.share_id,
                    compression_type: connection_result.compression_type,
                    enable_server_pointer: connection_result.enable_server_pointer,
                    pointer_software_rendering: connection_result.pointer_software_rendering,
                }
                .build();
                let mut image = DecodedImage::new(PixelFormat::RgbA32, DESKTOP_WIDTH, DESKTOP_HEIGHT);

                // The client advertises its capabilities when the channel opens;
                // the server handler seeing them means the channel is open there too.
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        tokio::select! {
                            caps = caps_rx.recv() => {
                                caps.expect("capabilities reached the EGFX handler");
                                break;
                            }
                            pdu = framed.read_pdu() => {
                                let (action, frame) = pdu.expect("read PDU");
                                for output in stage.process(&mut image, action, &frame).expect("stage process") {
                                    if let ActiveStageOutput::ResponseFrame(frame) = output {
                                        framed.write_all(&frame).await.expect("write response frame");
                                    }
                                }
                            }
                        }
                    }
                })
                .await
                .expect("the server opened the EGFX channel");
                let egfx_channel_id = egfx_channel
                    .lock()
                    .unwrap()
                    .expect("the client opened the EGFX channel");

                let counter = Arc::new(AtomicU64::new(0));
                let batch = b"tcp write-progress batch".to_vec();
                let weight = u64::try_from(batch.len()).expect("batch length fits in u64");
                let messages = ironrdp_dvc::encode_dvc_messages(
                    egfx_channel_id,
                    vec![Box::new(RawDvcPayload(batch.clone()))],
                    ironrdp::svc::ChannelFlags::empty(),
                )
                .expect("encode EGFX batch");
                ev.send(ServerEvent::Egfx(
                    ironrdp_server::EgfxServerMessage::send_messages(messages)
                        .with_write_counter(Arc::clone(&counter), weight),
                ))
                .unwrap();

                // Drive TCP until the batch itself reaches the client. The
                // channel's own CapsConfirm (the synchronous reply to the
                // client's initial CapsAdvertise) arrives first and is not
                // our batch, so match on content rather than position.
                tokio::time::timeout(Duration::from_secs(10), async {
                    while !egfx_received.lock().unwrap().contains(&batch) {
                        let (action, frame) = framed.read_pdu().await.expect("read PDU");
                        for output in stage.process(&mut image, action, &frame).expect("stage process") {
                            if let ActiveStageOutput::ResponseFrame(frame) = output {
                                framed.write_all(&frame).await.expect("write response frame");
                            }
                        }
                    }
                })
                .await
                .expect("the EGFX batch arrived over TCP");

                // The server advances the counter right after its own
                // `write_all` returns, which races the client's read above --
                // a bare assert here could sample before that increment lands.
                tokio::time::timeout(Duration::from_secs(5), async {
                    while counter.load(Ordering::Relaxed) != weight {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("the write-progress counter advanced by the batch's weight exactly once");

                // A second batch sent with no counter attached must not touch
                // the first one's -- proving `None` is a true no-op, not an
                // accidental reuse of the same counter.
                let messages = ironrdp_dvc::encode_dvc_messages(
                    egfx_channel_id,
                    vec![Box::new(RawDvcPayload(b"uncounted batch".to_vec()))],
                    ironrdp::svc::ChannelFlags::empty(),
                )
                .expect("encode EGFX batch");
                ev.send(ServerEvent::Egfx(ironrdp_server::EgfxServerMessage::send_messages(
                    messages,
                )))
                .unwrap();
                let second_batch = b"uncounted batch".to_vec();
                tokio::time::timeout(Duration::from_secs(10), async {
                    while !egfx_received.lock().unwrap().contains(&second_batch) {
                        let (action, frame) = framed.read_pdu().await.expect("read PDU");
                        for output in stage.process(&mut image, action, &frame).expect("stage process") {
                            if let ActiveStageOutput::ResponseFrame(frame) = output {
                                framed.write_all(&frame).await.expect("write response frame");
                            }
                        }
                    }
                })
                .await
                .expect("the second EGFX batch arrived over TCP");
                assert_eq!(
                    counter.load(Ordering::Relaxed),
                    weight,
                    "a batch sent with no write_counter must not touch someone else's"
                );

                // Client-initiated shutdown (the file's established pattern,
                // see `client_server_with_connector`): the server ends the
                // connection on its own once it sees this, so the single
                // `Quit` below lands on `run()`'s now-idle accept loop
                // instead of racing its per-connection dispatch.
                let outputs = stage.graceful_shutdown().expect("shutdown");
                for out in outputs {
                    match out {
                        ActiveStageOutput::ResponseFrame(frame) => {
                            framed.write_all(&frame).await.expect("write frame");
                        }
                        _ => unimplemented!(),
                    }
                }

                // server should probably send TLS close_notify
                while let Ok(pdu) = framed.read_pdu().await {
                    debug!(?pdu);
                }
                ev.send(ServerEvent::Quit("bye".into())).unwrap();
            });

            tokio::try_join!(server_task, client).expect("join");
        })
        .await;
}

/// Server EGFX handler that reports the client's advertised capabilities.
struct TestGfxFactory {
    caps_tx: UnboundedSender<()>,
}

impl ServerEventSender for TestGfxFactory {
    fn set_sender(&mut self, _sender: UnboundedSender<ServerEvent>) {}
}

impl GfxServerFactory for TestGfxFactory {
    fn build_gfx_handler(&self) -> Box<dyn ironrdp_egfx::server::GraphicsPipelineHandler> {
        Box::new(TestGfxHandler {
            caps_tx: self.caps_tx.clone(),
        })
    }
}

struct TestGfxHandler {
    caps_tx: UnboundedSender<()>,
}

impl ironrdp_egfx::server::GraphicsPipelineHandler for TestGfxHandler {
    fn capabilities_advertise(&mut self, _pdu: &CapabilitiesAdvertisePdu) {
        let _ = self.caps_tx.send(());
    }

    fn on_ready(&mut self, _negotiated: &CapabilitySet) {}
}

/// Client end of the EGFX channel that records what the server sends on it.
#[derive(Default)]
struct TestEgfxClient {
    channel_id: Arc<StdMutex<Option<u32>>>,
    received: Arc<StdMutex<Vec<Vec<u8>>>>,
}

impl_as_any!(TestEgfxClient);

impl DvcProcessor for TestEgfxClient {
    fn channel_name(&self) -> &str {
        "Microsoft::Windows::RDS::Graphics"
    }

    fn start(&mut self, channel_id: u32) -> pdu::PduResult<Vec<DvcMessage>> {
        *self.channel_id.lock().unwrap() = Some(channel_id);
        Ok(vec![Box::new(v8_capabilities())])
    }

    fn process(&mut self, _channel_id: u32, payload: &[u8]) -> pdu::PduResult<Vec<DvcMessage>> {
        self.received.lock().unwrap().push(payload.to_vec());
        Ok(Vec::new())
    }
}

impl DvcClientProcessor for TestEgfxClient {}

fn v8_capabilities() -> GfxPdu {
    GfxPdu::CapabilitiesAdvertise(CapabilitiesAdvertisePdu::from_typed(&[CapabilitySet::V8 {
        flags: CapabilitiesV8Flags::empty(),
    }]))
}

struct RawDvcPayload(Vec<u8>);

impl ironrdp::core::Encode for RawDvcPayload {
    fn encode(&self, dst: &mut ironrdp::core::WriteCursor<'_>) -> ironrdp::core::EncodeResult<()> {
        ironrdp::core::ensure_size!(in: dst, size: self.0.len());
        dst.write_slice(&self.0);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "RawDvcPayload"
    }

    fn size(&self) -> usize {
        self.0.len()
    }
}

impl DvcEncode for RawDvcPayload {}

pub(super) fn default_client_config() -> connector::Config {
    connector::Config {
        desktop_size: DesktopSize {
            width: DESKTOP_WIDTH,
            height: DESKTOP_HEIGHT,
        },
        monitor_layout: None,
        desktop_scale_factor: 0, // Default to 0 per FreeRDP
        enable_tls: true,
        enable_credssp: true,
        enable_standard_rdp_security: false,
        credentials: connector::Credentials::UsernamePassword {
            username: USERNAME.into(),
            password: PASSWORD.into(),
        },
        domain: None,
        client_build: semver::Version::parse(env!("CARGO_PKG_VERSION"))
            .map(|version| version.major * 100 + version.minor * 10 + version.patch)
            .unwrap_or(0)
            .try_into()
            .unwrap(),
        client_name: "ironrdp".into(),
        keyboard_type: gcc::KeyboardType::IBM_ENHANCED,
        keyboard_subtype: 0,
        keyboard_layout: 0,
        keyboard_functional_keys_count: 12,
        connection_type: gcc::ConnectionType::Lan,
        ime_file_name: "".into(),
        bitmap: None,
        dig_product_id: "".into(),
        // NOTE: hardcode this value like in freerdp
        // https://github.com/FreeRDP/FreeRDP/blob/4e24b966c86fdf494a782f0dfcfc43a057a2ea60/libfreerdp/core/settings.c#LL49C34-L49C70
        client_dir: "C:\\Windows\\System32\\mstscax.dll".into(),
        #[cfg(windows)]
        platform: MajorPlatformType::WINDOWS,
        #[cfg(target_os = "macos")]
        platform: MajorPlatformType::MACINTOSH,
        #[cfg(target_os = "ios")]
        platform: MajorPlatformType::IOS,
        #[cfg(target_os = "linux")]
        platform: MajorPlatformType::UNIX,
        #[cfg(target_os = "android")]
        platform: MajorPlatformType::ANDROID,
        #[cfg(target_os = "freebsd")]
        platform: MajorPlatformType::UNIX,
        #[cfg(target_os = "dragonfly")]
        platform: MajorPlatformType::UNIX,
        #[cfg(target_os = "openbsd")]
        platform: MajorPlatformType::UNIX,
        #[cfg(target_os = "netbsd")]
        platform: MajorPlatformType::UNIX,
        hardware_id: None,
        request_data: None,
        autologon: false,
        enable_audio_playback: true,
        enable_audio_capture: false,
        license_cache: None,
        compression_type: None,
        enable_server_pointer: true,
        pointer_software_rendering: true,
        multitransport_flags: None,
        support_dyn_vc_gfx_protocol: false,
        performance_flags: Default::default(),
        timezone_info: Default::default(),
        alternate_shell: String::new(),
        work_dir: String::new(),
        remote_application_mode: false,
        rail_support_level: pdu::rdp::capability_sets::RailSupportLevel::SUPPORTED,
    }
}

/// Drive a real client through the full handshake against `server_addr`
/// (TLS upgrade, CredSSP, finalization) and return its open transport and the
/// connection result. `make_connector` receives the client's local address.
async fn connect_client(
    server_addr: SocketAddr,
    make_connector: impl FnOnce(SocketAddr) -> connector::ClientConnector,
) -> (Framed<TokioStream<TlsStream<TcpStream>>>, connector::ConnectionResult) {
    let tcp_stream = TcpStream::connect(server_addr).await.expect("TCP connect");
    let client_addr = tcp_stream.local_addr().expect("local_addr");
    let mut framed = ironrdp_tokio::TokioFramed::new(tcp_stream);
    let mut connector = make_connector(client_addr);
    let should_upgrade = ironrdp_async::connect_begin(&mut framed, &mut connector)
        .await
        .expect("begin connection");
    let initial_stream = framed.into_inner_no_leftover();
    let (upgraded_stream, tls_cert) = ironrdp_tls::upgrade_with_certificate_validation(
        initial_stream,
        "localhost",
        ironrdp_tls::CertificateValidation::DangerouslyAcceptInvalidCertificate,
    )
    .await
    .expect("TLS upgrade");
    let upgraded = ironrdp_tokio::mark_as_upgraded(should_upgrade, &mut connector);
    let mut upgraded_framed = ironrdp_tokio::TokioFramed::new(upgraded_stream);
    let server_public_key = ironrdp_tls::extract_tls_server_public_key(&tls_cert).expect("extract server public key");
    let connection_result = ironrdp_async::connect_finalize(
        upgraded,
        connector,
        &mut upgraded_framed,
        &mut ironrdp_tokio::reqwest::ReqwestNetworkClient::new(),
        "localhost".into(),
        server_public_key.to_owned(),
        None,
    )
    .await
    .expect("finalize connection");
    (upgraded_framed, connection_result)
}

/// Counts how often each `ConnectionHandler` hook reached the handler.
/// `on_accept` fires before a connection is served, `on_connection_info`
/// mid-connection, `on_disconnected` after it ends.
#[derive(Default)]
struct HookCounts {
    accept: AtomicUsize,
    info: AtomicUsize,
    disconnected: AtomicUsize,
}

impl HookCounts {
    fn get(&self) -> (usize, usize, usize) {
        (
            self.accept.load(Ordering::SeqCst),
            self.info.load(Ordering::SeqCst),
            self.disconnected.load(Ordering::SeqCst),
        )
    }

    /// Poll until `done` holds for the counts, or a generous deadline passes.
    async fn wait_until(&self, done: impl Fn((usize, usize, usize)) -> bool) -> (usize, usize, usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done(self.get()) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        self.get()
    }
}

struct HookRecorder(Arc<HookCounts>);

impl server::ConnectionHandler for HookRecorder {
    fn on_accept(&mut self, _peer: SocketAddr) -> bool {
        self.0.accept.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn on_connection_info(&mut self, _info: &server::ConnectionInfo) {
        self.0.info.fetch_add(1, Ordering::SeqCst);
    }

    fn on_disconnected(
        &mut self,
        _peer: SocketAddr,
        _duration: Duration,
        _error: Option<&server::ServerError>,
    ) -> server::PostConnectionAction {
        self.0.disconnected.fetch_add(1, Ordering::SeqCst);
        server::PostConnectionAction::Continue
    }
}

/// A TLS server under `policy` whose connection handler records into the
/// returned counts. Keep the returned display sender alive for the test: once
/// it drops, the display stream ends and the server closes the session.
fn hook_recording_server(
    policy: server::ConnectionPolicy,
) -> (RdpServer, Arc<HookCounts>, UnboundedSender<DisplayUpdate>) {
    let cert_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-cert.pem");
    let key_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-key.pem");
    let identity = TlsIdentityCtx::init_from_paths(&cert_path, &key_path).expect("failed to init TLS identity");
    let acceptor = identity.make_acceptor().expect("failed to build TLS acceptor");

    let counts = Arc::new(HookCounts::default());
    let (display_tx, display_rx) = mpsc::unbounded_channel();
    let mut server = RdpServer::builder()
        .with_addr(([127, 0, 0, 1], 0))
        .with_tls(acceptor)
        .with_input_handler(TestInputHandler)
        .with_display_handler(TestDisplay {
            rx: Arc::new(Mutex::new(display_rx)),
        })
        .with_connection_handler(Some(Box::new(HookRecorder(Arc::clone(&counts)))))
        .with_connection_policy(policy)
        .build();
    server.set_credentials(Some(server::Credentials {
        username: USERNAME.into(),
        password: PASSWORD.into(),
        domain: None,
    }));
    (server, counts, display_tx)
}

/// Each connection's own [`server::ConnectionAbortHandle`], in arrival
/// order, as recorded by [`AbortHandleRecorder`].
type RecordedAbortHandles = Arc<StdMutex<Vec<server::ConnectionAbortHandle>>>;

/// A TLS server under `policy`, wired to an [`AbortHandleRecorder`] instead
/// of a plain [`HookRecorder`]: the same accept/info/disconnect counts, plus
/// each connection's own [`server::ConnectionAbortHandle`] (via
/// [`server::ConnectionInfo::abort_handle`]) captured in arrival order. Keep
/// the returned display sender alive, same reason as [`hook_recording_server`].
fn abort_handle_recording_server(
    policy: server::ConnectionPolicy,
) -> (
    RdpServer,
    Arc<HookCounts>,
    RecordedAbortHandles,
    UnboundedSender<DisplayUpdate>,
) {
    let cert_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-cert.pem");
    let key_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-key.pem");
    let identity = TlsIdentityCtx::init_from_paths(&cert_path, &key_path).expect("failed to init TLS identity");
    let acceptor = identity.make_acceptor().expect("failed to build TLS acceptor");

    let counts = Arc::new(HookCounts::default());
    let handles: RecordedAbortHandles = Arc::new(StdMutex::new(Vec::new()));
    let (display_tx, display_rx) = mpsc::unbounded_channel();
    let mut server = RdpServer::builder()
        .with_addr(([127, 0, 0, 1], 0))
        .with_tls(acceptor)
        .with_input_handler(TestInputHandler)
        .with_display_handler(TestDisplay {
            rx: Arc::new(Mutex::new(display_rx)),
        })
        .with_connection_handler(Some(Box::new(AbortHandleRecorder {
            counts: Arc::clone(&counts),
            handles: Arc::clone(&handles),
        })))
        .with_connection_policy(policy)
        .build();
    server.set_credentials(Some(server::Credentials {
        username: USERNAME.into(),
        password: PASSWORD.into(),
        domain: None,
    }));
    (server, counts, handles, display_tx)
}

async fn local_addr_of(ev: &UnboundedSender<ServerEvent>) -> SocketAddr {
    let (tx, rx) = oneshot::channel();
    ev.send(ServerEvent::GetLocalAddr(tx)).unwrap();
    rx.await.unwrap().unwrap()
}

/// Serve one real client through the full handshake under `policy` and report
/// `(on_accept reached the handler, on_connection_info reached the handler)`.
async fn hooks_reaching_handler_under(policy: server::ConnectionPolicy) -> (bool, bool) {
    let (mut server, counts, _display_tx) = hook_recording_server(policy);
    let ev = server.event_sender().clone();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let server_task = tokio::task::spawn_local(async move {
                let _ = server.run().await;
            });

            let server_addr = local_addr_of(&ev).await;
            let (client, _) = connect_client(server_addr, |client_addr| {
                connector::ClientConnector::new(default_client_config(), client_addr)
            })
            .await;

            // The client is now active, so the server has finished its side of
            // the handshake. Keep the connection open while the hook lands.
            let (accept, info, _) = counts.wait_until(|(_, info, _)| info >= 1).await;

            drop(client);
            server_task.abort();
            (accept >= 1, info >= 1)
        })
        .await
}

#[tokio::test]
async fn connection_handler_hooks_reach_handler_under_queue() {
    let (accept, info) = Box::pin(hooks_reaching_handler_under(server::ConnectionPolicy::Queue)).await;
    assert!(accept && info, "Queue: on_accept={accept}, on_connection_info={info}");
}

#[tokio::test]
async fn connection_handler_hooks_reach_handler_under_preempt() {
    let (accept, info) = Box::pin(hooks_reaching_handler_under(server::ConnectionPolicy::Preempt)).await;
    assert!(accept && info, "Preempt: on_accept={accept}, on_connection_info={info}");
}

#[tokio::test]
async fn connection_handler_hooks_reach_handler_under_reject() {
    let (accept, info) = Box::pin(hooks_reaching_handler_under(server::ConnectionPolicy::Reject)).await;
    assert!(accept && info, "Reject: on_accept={accept}, on_connection_info={info}");
}

/// The interleaving the shared handler exists for: under `Preempt`, a second
/// client's `on_accept` runs through the race's handle while the first session
/// is live, the newcomer takes over, the evicted session's `on_disconnected`
/// still lands, and the newcomer's own `on_connection_info` reaches the same
/// handler.
#[tokio::test]
async fn connection_handler_sees_every_hook_across_a_preemption() {
    let (mut server, counts, _display_tx) = hook_recording_server(server::ConnectionPolicy::Preempt);
    let ev = server.event_sender().clone();

    let local = tokio::task::LocalSet::new();
    Box::pin(local.run_until(async move {
        let server_task = tokio::task::spawn_local(async move {
            let _ = server.run().await;
        });
        let server_addr = local_addr_of(&ev).await;
        let connect = || {
            connect_client(server_addr, |client_addr| {
                connector::ClientConnector::new(default_client_config(), client_addr)
            })
        };

        let (mut first, _) = connect().await;
        assert_eq!(
            counts.wait_until(|(_, info, _)| info >= 1).await,
            (1, 1, 0),
            "first session: (on_accept, on_connection_info, on_disconnected)"
        );

        // The newcomer only finishes its handshake if it takes the session over.
        let (second, _) = tokio::time::timeout(Duration::from_secs(10), connect())
            .await
            .expect("the newcomer must take the session over");

        assert_eq!(
            counts.wait_until(|(_, info, disc)| info >= 2 && disc >= 1).await,
            (2, 2, 1),
            "after the takeover: both accepted, both reported connection info, the evicted one disconnected"
        );

        // And the evicted client's transport is closed, not left open.
        let evicted = tokio::time::timeout(Duration::from_secs(5), async {
            while first.read_pdu().await.is_ok() {}
        })
        .await;
        assert!(evicted.is_ok(), "the evicted client's connection must be closed");

        drop(second);
        server_task.abort();
    }))
    .await;
}

/// Per-session events left on the server-global channel by a connection that
/// ended must be dropped before the next connection is served, while the
/// handful of control events that outlive a connection survive. The returned
/// count makes the drop observable, and the fresh-path call site relies on
/// exactly this contract (see `discard_stale_session_events_runs_before_a_fresh_entry_too`
/// below).
#[tokio::test]
async fn discard_stale_session_events_keeps_lifecycle_events_and_returns_the_discarded_count() {
    let mut server = RdpServer::builder()
        .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
        .with_no_security()
        .with_no_input()
        .with_no_display()
        .build();

    // Two per-session leftovers (must go) around one control event (must stay).
    let sender = server.event_sender().clone();
    sender.send(ServerEvent::AutoDetectRttRequest).unwrap();
    sender.send(ServerEvent::SetAutoReconnectCookie(None)).unwrap();
    sender
        .send(ServerEvent::Disconnect(ErrorInfo::ProtocolIndependentCode(
            ProtocolIndependentCode::DisconnectedByOtherconnection,
        )))
        .unwrap();

    let dropped = server.__test_discard_stale_session_events().await;
    assert_eq!(dropped, 2, "the RttRequest and the stale Disconnect must be discarded");

    let remaining = server.__test_drain_pending_events().await;
    assert_eq!(
        remaining.len(),
        1,
        "exactly the lifecycle event should have survived the drain: {remaining:?}"
    );
    assert!(
        matches!(&remaining[0], ServerEvent::SetAutoReconnectCookie(None)),
        "the surviving event should be the control event, not a discarded per-session one: {remaining:?}"
    );
}

/// A `ConnectionHandler` whose first `on_disconnected` floods the
/// server-global channel with stale `Disconnect` events, standing in for
/// whatever a real session might leave behind uncollected (an eviction
/// notice, a queued clipboard/RDPSND/EGFX message). 32 of them, not 1: under
/// the pre-fix code the only thing standing between a brand new connection
/// and inheriting one of these is `run()`'s own idle-time `select!` picking
/// `recv` over `accept` on every single one of 32 independent, unbiased
/// coin flips -- a 1-in-2^32 chance of NOT leaking at least one. Flooding
/// turns "theoretically racy" into "fails close to every run" for a black-box
/// test that exercises the real `run()` loop rather than any instrumentation.
struct StaleEventInjector {
    counts: Arc<HookCounts>,
    ev_sender: Arc<StdMutex<Option<UnboundedSender<ServerEvent>>>>,
    injected: Arc<AtomicBool>,
}

impl server::ConnectionHandler for StaleEventInjector {
    fn on_accept(&mut self, _peer: SocketAddr) -> bool {
        self.counts.accept.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn on_connection_info(&mut self, _info: &server::ConnectionInfo) {
        self.counts.info.fetch_add(1, Ordering::SeqCst);
    }

    fn on_disconnected(
        &mut self,
        _peer: SocketAddr,
        _duration: Duration,
        _error: Option<&server::ServerError>,
    ) -> server::PostConnectionAction {
        self.counts.disconnected.fetch_add(1, Ordering::SeqCst);
        // Only the FIRST disconnect floods the channel: this must simulate
        // one ended session's leftovers reaching the NEXT connection, not
        // keep re-flooding on every subsequent disconnect in this test.
        if !self.injected.swap(true, Ordering::SeqCst)
            && let Some(sender) = self.ev_sender.lock().expect("lock").as_ref()
        {
            for _ in 0..32 {
                let _ = sender.send(ServerEvent::Disconnect(ErrorInfo::ProtocolIndependentCode(
                    ProtocolIndependentCode::DisconnectedByOtherconnection,
                )));
            }
        }
        server::PostConnectionAction::Continue
    }
}

/// A TLS server under `ConnectionPolicy::Queue`, wired to a
/// [`StaleEventInjector`]. Keep the returned display sender alive for the
/// test, same reason as [`hook_recording_server`].
fn stale_event_injecting_server() -> (RdpServer, Arc<HookCounts>, UnboundedSender<DisplayUpdate>) {
    let cert_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-cert.pem");
    let key_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-key.pem");
    let identity = TlsIdentityCtx::init_from_paths(&cert_path, &key_path).expect("failed to init TLS identity");
    let acceptor = identity.make_acceptor().expect("failed to build TLS acceptor");

    let counts = Arc::new(HookCounts::default());
    let ev_slot: Arc<StdMutex<Option<UnboundedSender<ServerEvent>>>> = Arc::new(StdMutex::new(None));
    let (display_tx, display_rx) = mpsc::unbounded_channel();
    let handler = StaleEventInjector {
        counts: Arc::clone(&counts),
        ev_sender: Arc::clone(&ev_slot),
        injected: Arc::new(AtomicBool::new(false)),
    };
    let mut server = RdpServer::builder()
        .with_addr(([127, 0, 0, 1], 0))
        .with_tls(acceptor)
        .with_input_handler(TestInputHandler)
        .with_display_handler(TestDisplay {
            rx: Arc::new(Mutex::new(display_rx)),
        })
        .with_connection_handler(Some(Box::new(handler)))
        .with_connection_policy(server::ConnectionPolicy::Queue)
        .build();
    server.set_credentials(Some(server::Credentials {
        username: USERNAME.into(),
        password: PASSWORD.into(),
        domain: None,
    }));
    // Only obtainable once the server exists; the handler reads through the
    // same slot once its first `on_disconnected` fires.
    *ev_slot.lock().expect("lock") = Some(server.event_sender().clone());
    (server, counts, display_tx)
}

/// `discard_stale_session_events` used to run only for a preemption winner --
/// the `Negotiated` arm of `run()`'s accept loop -- so a connection accepted
/// through the ordinary, non-preempting path (`Entry::Fresh`) could inherit
/// per-session leftovers (a stale `Disconnect`, among others) the previous
/// session never consumed. Drive that directly, through the public API,
/// against the real `run()` loop (no test-only instrumentation in
/// production code): connect A, let its first disconnect flood the channel
/// with 32 stale `Disconnect` events (see [`StaleEventInjector`]), have B
/// already waiting to be accepted when that flood lands, and require B to
/// come up cleanly regardless -- then prove a CURRENT `Disconnect` still
/// reaches B, so this isn't just "B's handler stopped seeing events".
#[tokio::test]
async fn a_fresh_entry_does_not_inherit_the_previous_sessions_stale_events() {
    let (mut server, counts, _display_tx) = stale_event_injecting_server();
    let ev = server.event_sender().clone();

    let local = tokio::task::LocalSet::new();
    Box::pin(local.run_until(async move {
        let server_task = tokio::task::spawn_local(async move {
            let _ = server.run().await;
        });
        let server_addr = local_addr_of(&ev).await;
        let connect = || {
            connect_client(server_addr, |client_addr| {
                connector::ClientConnector::new(default_client_config(), client_addr)
            })
        };

        let (a, _) = connect().await;
        assert_eq!(
            counts.wait_until(|(_, info, _)| info >= 1).await,
            (1, 1, 0),
            "A must be live before we end it"
        );

        // B's TCP connect sits in the listener backlog: under `Queue`,
        // `run()` does not call `listener.accept()` again until A's own
        // connection future resolves, so B is fully established at the OS
        // level but unserved for as long as A is live -- exactly the
        // condition under which `run()`'s idle-time `select!` could
        // previously race `recv` (the flood, once A ends) against `accept`
        // (B) and leak into B.
        let b_connect = tokio::task::spawn_local(async move {
            connect_client(server_addr, |client_addr| {
                connector::ClientConnector::new(default_client_config(), client_addr)
            })
            .await
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Ends A, whose `on_disconnected` floods the channel with 32 stale
        // `Disconnect`s right as `run()` is about to loop back and decide
        // what B's entry is.
        drop(a);

        let (accept, info, _) = tokio::time::timeout(
            Duration::from_secs(5),
            counts.wait_until(|(accept, info, _)| accept >= 2 && info >= 2),
        )
        .await
        .expect("B must still be served and complete its handshake despite the flood");
        // A real margin, not an immediate check: a leaked stale event would
        // reach B's own dispatch loop and end it very soon after `on_connection_info`,
        // not instantly, so assert on `disconnected` only after waiting past
        // that window.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            (accept, info, counts.get().2),
            (2, 2, 1),
            "B must come up clean: still only A disconnected, 500ms after B's own handshake completed"
        );

        let (b, _) = tokio::time::timeout(Duration::from_secs(5), b_connect)
            .await
            .expect("join")
            .expect("B's handshake must actually succeed, not be torn down by a leaked stale event");

        // A CURRENT event must still reach B: this isn't "B stopped seeing
        // server events at all", it's specifically the stale ones that must
        // not reach it.
        ev.send(ServerEvent::Disconnect(ErrorInfo::ProtocolIndependentCode(
            ProtocolIndependentCode::DisconnectedByOtherconnection,
        )))
        .unwrap();
        let disconnected_after_fresh_event =
            tokio::time::timeout(Duration::from_secs(5), counts.wait_until(|(_, _, disc)| disc >= 2))
                .await
                .expect("a fresh Disconnect sent for the live connection must still end it")
                .2;
        assert_eq!(disconnected_after_fresh_event, 2);

        drop(b);
        server_task.abort();
    }))
    .await;
}

/// A connection that never ends on its own -- no more PDUs, no display
/// updates, no server events -- must still be droppable: the embedder's
/// watchdog calls `ConnectionAbortHandle::abort()` (obtained from
/// `ConnectionInfo::abort_handle()`, the only route -- there is no
/// server-scoped convenience) when `on_disconnected` is overdue, and that
/// must make the connection future resolve so `on_disconnected` teardown
/// actually runs, rather than hang forever waiting for a peer or an event
/// that will never come.
///
/// Drives one wedged connection under `policy` and returns the final
/// `(accept, info, disconnected)` counts, same shape as
/// [`hooks_reaching_handler_under`]: a single connection is served
/// regardless of policy, so this exercises exactly one of the six
/// `serve_with_abort`-wrapped call sites per policy (each policy's own
/// `Fresh` arm -- `Negotiated` is unreachable without a second, preempting
/// connection, which this test does not drive).
async fn connection_abort_handle_drops_a_wedged_connection_under(
    policy: server::ConnectionPolicy,
) -> (usize, usize, usize) {
    let (mut server, counts, handles, _display_tx) = abort_handle_recording_server(policy);
    let ev = server.event_sender().clone();

    let local = tokio::task::LocalSet::new();
    Box::pin(local.run_until(async move {
        let server_task = tokio::task::spawn_local(async move {
            let _ = server.run().await;
        });
        let server_addr = local_addr_of(&ev).await;
        let (client, _) = connect_client(server_addr, |client_addr| {
            connector::ClientConnector::new(default_client_config(), client_addr)
        })
        .await;

        assert_eq!(
            counts.wait_until(|(_, info, _)| info >= 1).await,
            (1, 1, 0),
            "the connection must be live before we try to abort it"
        );
        let abort = handles.lock().expect("lock")[0].clone();

        // Confirm it is genuinely wedged: idle, nobody reads or writes
        // anything further, so without an abort it would just sit here.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            counts.get().2,
            0,
            "the connection must not end on its own while idle -- otherwise this test would not be exercising the abort at all"
        );

        abort.abort();

        let counts = tokio::time::timeout(Duration::from_secs(5), counts.wait_until(|(_, _, disc)| disc >= 1))
            .await
            .expect("the abort must make the wedged connection's teardown run");

        drop(client);
        server_task.abort();
        counts
    }))
    .await
}

#[tokio::test]
async fn connection_abort_handle_drops_a_wedged_connection_under_queue() {
    let (_, _, disconnected) =
        connection_abort_handle_drops_a_wedged_connection_under(server::ConnectionPolicy::Queue).await;
    assert_eq!(
        disconnected, 1,
        "on_disconnected must fire exactly once for the aborted connection"
    );
}

#[tokio::test]
async fn connection_abort_handle_drops_a_wedged_connection_under_preempt() {
    // A session-takeover embedder runs under this policy: a regression here
    // would silently remove the hard-close path without affecting `Queue`.
    let (_, _, disconnected) =
        connection_abort_handle_drops_a_wedged_connection_under(server::ConnectionPolicy::Preempt).await;
    assert_eq!(
        disconnected, 1,
        "on_disconnected must fire exactly once for the aborted connection"
    );
}

#[tokio::test]
async fn connection_abort_handle_drops_a_wedged_connection_under_reject() {
    let (_, _, disconnected) =
        connection_abort_handle_drops_a_wedged_connection_under(server::ConnectionPolicy::Reject).await;
    assert_eq!(
        disconnected, 1,
        "on_disconnected must fire exactly once for the aborted connection"
    );
}

/// A `ConnectionHandler` that records each connection's
/// `ConnectionInfo::abort_handle()` in order, so a test can retrieve A's and
/// B's handles separately after the fact.
struct AbortHandleRecorder {
    counts: Arc<HookCounts>,
    handles: RecordedAbortHandles,
}

impl server::ConnectionHandler for AbortHandleRecorder {
    fn on_accept(&mut self, _peer: SocketAddr) -> bool {
        self.counts.accept.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn on_connection_info(&mut self, info: &server::ConnectionInfo) {
        self.counts.info.fetch_add(1, Ordering::SeqCst);
        self.handles.lock().expect("lock").push(info.abort_handle());
    }

    fn on_disconnected(
        &mut self,
        _peer: SocketAddr,
        _duration: Duration,
        _error: Option<&server::ServerError>,
    ) -> server::PostConnectionAction {
        self.counts.disconnected.fetch_add(1, Ordering::SeqCst);
        server::PostConnectionAction::Continue
    }
}

/// `ConnectionInfo::abort_handle()` is scoped to the one connection it was
/// issued for, by construction, unlike `RdpServer::connection_abort_handle()`'s
/// server-scoped convenience (covered above, used immediately in every
/// case): a handle retained past the connection it was built for must be
/// permanently inert, even though another connection is later served through
/// the very same `RdpServer`. `ConnectionPolicy::Queue`, so B is only
/// accepted once A has fully ended -- the exact "late abort meant for A
/// arrives after B started" shape this guards against.
#[tokio::test]
async fn connection_info_abort_handle_cannot_reach_a_later_connection() {
    let cert_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-cert.pem");
    let key_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-key.pem");
    let identity = TlsIdentityCtx::init_from_paths(&cert_path, &key_path).expect("failed to init TLS identity");
    let acceptor = identity.make_acceptor().expect("failed to build TLS acceptor");

    let counts = Arc::new(HookCounts::default());
    let handles: RecordedAbortHandles = Arc::new(StdMutex::new(Vec::new()));
    // Kept alive (bound, never dropped) for the whole test: dropping it ends
    // the display stream and closes the session, same reason as
    // `hook_recording_server`.
    let (_display_tx, display_rx) = mpsc::unbounded_channel();
    let mut server = RdpServer::builder()
        .with_addr(([127, 0, 0, 1], 0))
        .with_tls(acceptor)
        .with_input_handler(TestInputHandler)
        .with_display_handler(TestDisplay {
            rx: Arc::new(Mutex::new(display_rx)),
        })
        .with_connection_handler(Some(Box::new(AbortHandleRecorder {
            counts: Arc::clone(&counts),
            handles: Arc::clone(&handles),
        })))
        .with_connection_policy(server::ConnectionPolicy::Queue)
        .build();
    server.set_credentials(Some(server::Credentials {
        username: USERNAME.into(),
        password: PASSWORD.into(),
        domain: None,
    }));
    let ev = server.event_sender().clone();

    let local = tokio::task::LocalSet::new();
    Box::pin(local.run_until(async move {
        let server_task = tokio::task::spawn_local(async move {
            let _ = server.run().await;
        });
        let server_addr = local_addr_of(&ev).await;
        let connect = || {
            connect_client(server_addr, |client_addr| {
                connector::ClientConnector::new(default_client_config(), client_addr)
            })
        };

        let (a, _) = connect().await;
        assert_eq!(counts.wait_until(|(_, info, _)| info >= 1).await, (1, 1, 0));
        let a_handle = handles.lock().expect("lock")[0].clone();

        // End A on its own terms (not via its handle), so its own
        // `on_disconnected` is the only thing that bumps the count to 1
        // before B exists at all.
        drop(a);
        counts.wait_until(|(_, _, disc)| disc >= 1).await;

        let (_b, _) = connect().await;
        assert_eq!(
            counts.wait_until(|(accept, info, _)| accept >= 2 && info >= 2).await,
            (2, 2, 1),
            "B must be live before we try A's stale handle against it"
        );

        // The late abort: A's handle, retained past A's own lifetime, must
        // not touch B.
        a_handle.abort();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            counts.get(),
            (2, 2, 1),
            "A's stale handle must be a permanent no-op, even with B now live"
        );

        // Prove the mechanism is not simply inert outright: B's OWN handle
        // must still work.
        let b_handle = handles.lock().expect("lock")[1].clone();
        b_handle.abort();
        let final_counts = tokio::time::timeout(Duration::from_secs(5), counts.wait_until(|(_, _, disc)| disc >= 2))
            .await
            .expect("B's own handle must still abort B");
        assert_eq!(final_counts, (2, 2, 2));

        server_task.abort();
    }))
    .await;
}

/// N1: the per-connection guarantee depends on `connection_abort` being
/// reminted BEFORE a connection's future is built, not after it completes --
/// otherwise a serve that is CANCELLED (its future dropped without
/// completing) never reaches the remint at all, and the next call shares
/// the SAME `Notify` with the one that got cancelled. Drive exactly that
/// through `run_connection_with`, the embedder-driven path (not `run()`):
/// an embedder with its own accept loop cancels its call for A mid-session,
/// then serves B through the same `RdpServer` with another call. A's stale
/// handle must not reach B.
#[tokio::test]
async fn a_cancelled_run_connection_with_does_not_let_a_stale_handle_reach_the_next_connection() {
    let cert_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-cert.pem");
    let key_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/server-key.pem");
    let identity = TlsIdentityCtx::init_from_paths(&cert_path, &key_path).expect("failed to init TLS identity");
    let acceptor = identity.make_acceptor().expect("failed to build TLS acceptor");

    let counts = Arc::new(HookCounts::default());
    let handles: RecordedAbortHandles = Arc::new(StdMutex::new(Vec::new()));
    let (_display_tx, display_rx) = mpsc::unbounded_channel();
    let mut server = RdpServer::builder()
        .with_addr(([127, 0, 0, 1], 0))
        .with_tls(acceptor)
        .with_input_handler(TestInputHandler)
        .with_display_handler(TestDisplay {
            rx: Arc::new(Mutex::new(display_rx)),
        })
        .with_connection_handler(Some(Box::new(AbortHandleRecorder {
            counts: Arc::clone(&counts),
            handles: Arc::clone(&handles),
        })))
        .build();
    server.set_credentials(Some(server::Credentials {
        username: USERNAME.into(),
        password: PASSWORD.into(),
        domain: None,
    }));

    let local = tokio::task::LocalSet::new();
    Box::pin(local.run_until(async move {
        // `RdpServer::run()` is not used at all here: this models an
        // embedder with its own accept loop, calling `run_connection_with`
        // directly, which is the exact shape N1 was reproduced through.
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let server_addr = listener.local_addr().expect("local_addr");

        let server_task = tokio::task::spawn_local(async move {
            let (stream_a, _) = listener.accept().await.expect("accept A");
            let _ = stream_a.set_nodelay(true);
            // Cancel A's serve mid-session: an idle connection (no PDUs, no
            // display updates, no server events) never finishes on its own,
            // so this timeout always fires and drops `run_connection_with`'s
            // future before it completes -- the exact shape that skipped
            // the old end-of-serve remint.
            let _ = tokio::time::timeout(
                Duration::from_millis(300),
                server.run_connection_with(stream_a, server::TransportTls::Managed),
            )
            .await;

            let (stream_b, _) = listener.accept().await.expect("accept B");
            let _ = stream_b.set_nodelay(true);
            // No outer timeout on B: idle like A, it must hang here until
            // something ends it. If A's stale handle reaches it, this
            // returns early; if not, it only returns once B's OWN handle
            // (below) aborts it.
            let _ = server
                .run_connection_with(stream_b, server::TransportTls::Managed)
                .await;
        });

        let connect = || {
            connect_client(server_addr, |client_addr| {
                connector::ClientConnector::new(default_client_config(), client_addr)
            })
        };

        // Held until the very end, deliberately: dropping it early would
        // close A's socket from the client side, which ends A's serve
        // through a normal read error instead of the cancellation this test
        // means to exercise -- a normal end reminted the `Notify` in BOTH
        // the buggy and fixed designs, which would make this test pass
        // either way.
        let (a, _) = connect().await;
        assert_eq!(
            counts.wait_until(|(_, info, _)| info >= 1).await.1,
            1,
            "A must complete its handshake before its serve is cancelled"
        );
        let a_handle = handles.lock().expect("lock")[0].clone();

        let (b, _) = tokio::time::timeout(Duration::from_secs(5), connect())
            .await
            .expect("B must be accepted and complete its handshake after A's serve is cancelled");
        assert_eq!(
            counts.wait_until(|(_, info, _)| info >= 2).await.1,
            2,
            "B's own on_connection_info must have fired"
        );
        let b_handle = handles.lock().expect("lock")[1].clone();

        // The late abort: A's handle, from a CANCELLED serve, must not touch B.
        a_handle.abort();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !server_task.is_finished(),
            "A's stale handle must not end B's still-running run_connection_with call"
        );

        // Prove the mechanism still works: B's own handle must end it.
        b_handle.abort();
        tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("B's own handle must make its run_connection_with call return")
            .expect("join");

        drop(a);
        drop(b);
    }))
    .await;
}

/// A test double whose writes never complete, standing in for a peer that
/// stopped reading: the kernel send buffer eventually fills and `write_all`
/// blocks, which here is modeled directly rather than via real TCP
/// backpressure (a single `ServerSetErrorInfoPdu` is far too small to fill a
/// loopback socket's buffer in a fast, deterministic test).
struct WedgedWriter;

impl ironrdp_async::FramedWrite for WedgedWriter {
    type WriteAllFut<'write> = core::future::Pending<std::io::Result<()>>;

    fn write_all<'a>(&'a mut self, _buf: &'a [u8]) -> Self::WriteAllFut<'a> {
        core::future::pending()
    }
}

/// A server-initiated disconnect ([`ServerEvent::Disconnect`]) must put the
/// reason on the wire on a bounded write and then disconnect even if the
/// peer never reads, so a stalled client cannot wedge the dispatch loop just
/// because it has not acknowledged why it is being disconnected.
#[tokio::test]
async fn a_server_initiated_disconnect_is_bounded_when_the_peer_stops_reading() {
    let mut server = RdpServer::builder()
        .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
        .with_no_security()
        .with_no_input()
        .with_no_display()
        .build();

    let mut events = vec![ServerEvent::Disconnect(ErrorInfo::ProtocolIndependentCode(
        ProtocolIndependentCode::ServerDeniedConnection,
    ))];
    let mut writer = WedgedWriter;

    let is_disconnect = tokio::time::timeout(
        Duration::from_secs(2),
        server.__test_dispatch_server_events(&mut events, &mut writer, 1003, 1002, None),
    )
    .await
    .expect("the bounded SetErrorInfo write must return even when the peer never reads")
    .expect("a bounded disconnect write must not surface as a session error");

    assert!(is_disconnect);
}

/// Bounding the `Disconnect` write alone is not enough: the TLS `shutdown()`
/// that runs immediately afterward (`finalize_and_shutdown`, once the
/// dispatch loop has actually exited) queues `close_notify` and then loops
/// writing until rustls's internal buffer drains, which blocks on a peer
/// that stopped reading exactly like any other write -- just one step later
/// than the one that is already bounded. Drive this over a REAL TLS
/// connection (not `__test_dispatch_server_events`, which stops at the
/// dispatch loop's own return and never reaches `finalize_and_shutdown` at
/// all): flood the display path with enough distinct, incompressible frames
/// to fill the actual TCP+TLS pipe, stop the peer from reading anything,
/// then disconnect. `on_disconnected` must land within a couple of bounded
/// grace windows, not hang on a peer that may never read again.
#[tokio::test]
async fn a_server_initiated_disconnect_tears_down_promptly_over_tls_when_the_peer_stops_reading() {
    let (mut server, counts, display_tx) = hook_recording_server(server::ConnectionPolicy::Queue);
    let ev = server.event_sender().clone();

    let local = tokio::task::LocalSet::new();
    Box::pin(local.run_until(async move {
        let server_task = tokio::task::spawn_local(async move {
            let _ = server.run().await;
        });
        let server_addr = local_addr_of(&ev).await;
        let (client, _) = connect_client(server_addr, |client_addr| {
            connector::ClientConnector::new(default_client_config(), client_addr)
        })
        .await;
        // Held for the rest of the test, but never read again: models a peer
        // that stopped consuming entirely.
        let _client = client;

        assert_eq!(
            counts.wait_until(|(_, info, _)| info >= 1).await,
            (1, 1, 0),
            "the connection must be live before we flood and disconnect it"
        );

        // Each frame distinct (a per-frame nonce mixed into a cheap
        // multiplicative hash -- no `rand` dependency here, and genuine
        // randomness is not the point): identical consecutive frames would
        // let a diff/compression step collapse them to almost nothing,
        // defeating the flood. 96 full-desktop frames is far more than
        // enough to exceed any realistic loopback socket buffer even after
        // compression. The HIGH byte of the multiply (`to_le_bytes()[3]`),
        // not the low one: in `i.wrapping_mul(CONST)`, the low byte depends
        // only on `i mod 256` (multiplication mod 2^8 only sees the low 8
        // bits of each operand), so it repeats every 256 bytes -- a classic
        // LCG weakness. The high byte depends on nearly all of `i`'s bits
        // (via carry propagation through the multiplication) and does not
        // repeat at any practical period, which is what "incompressible"
        // actually requires here.
        let frame_len = usize::from(DESKTOP_WIDTH) * usize::from(DESKTOP_HEIGHT) * 4;
        for frame_idx in 0..96u32 {
            let nonce = frame_idx.wrapping_mul(0x9E37_79B9);
            let data: Vec<u8> = (0..frame_len)
                .map(|i| {
                    let i = u32::try_from(i).unwrap_or(u32::MAX);
                    (i.wrapping_mul(2_654_435_761) ^ nonce).to_le_bytes()[3]
                })
                .collect();
            let _ = display_tx.send(DisplayUpdate::Bitmap(BitmapUpdate {
                x: 0,
                y: 0,
                width: core::num::NonZeroU16::new(DESKTOP_WIDTH).expect("nonzero"),
                height: core::num::NonZeroU16::new(DESKTOP_HEIGHT).expect("nonzero"),
                format: PixelFormat::BgrX32,
                data: data.into(),
                stride: core::num::NonZeroUsize::new(usize::from(DESKTOP_WIDTH) * 4).expect("nonzero"),
            }));
        }

        // Give the dispatch loop a moment to start pushing frames onto a
        // socket nobody is draining, so the write that `Disconnect` lands
        // behind is actually blocked, not merely queued.
        tokio::time::sleep(Duration::from_millis(300)).await;

        ev.send(ServerEvent::Disconnect(ErrorInfo::ProtocolIndependentCode(
            ProtocolIndependentCode::ServerDeniedConnection,
        )))
        .unwrap();

        let (_, _, disconnected) = tokio::time::timeout(
            Duration::from_secs(3),
            counts.wait_until(|(_, _, disc)| disc >= 1),
        )
        .await
        .expect(
            "teardown over TLS must complete within a couple of bounded-grace windows, not hang on the stalled peer",
        );
        assert_eq!(disconnected, 1);

        server_task.abort();
    }))
    .await;
}
