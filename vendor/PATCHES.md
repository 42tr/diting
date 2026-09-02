# Vendored patches

crates.io 上 livekit Rust SDK 各 crate 独立发布、版本互相对不齐。本目录收录三个打过兼容补丁的
crate 副本，通过根 `Cargo.toml` 的 `[patch.crates-io]` 生效。官方发布对齐版本后，删除
`[patch.crates-io]` 段并升级即可卸载这些补丁。

## livekit-api-0.5.6

- **文件**: `src/services/connector.rs`
- **问题**: `livekit-protocol 0.7.12` 给 `ConnectWhatsAppCallRequest` 新增必填字段
  `wait_until_answered`,0.5.6 用结构体字面量构造它 → E0063。protocol 因
  `livekit-common 0.1.1` 要求 `^0.7.12` 无法降级。
- **补丁**: 构造处补 `wait_until_answered: false,`

## livekit-0.7.52

- **文件**: `src/proto.rs`
- **问题**: `livekit-protocol 0.7.12` 的 `participant_info::KindDetail` 新增 `Simulation`
  变体,0.7.52 的 `From` match 未覆盖 → E0004。
- **补丁**: 补 arm `KindDetail::Simulation => ParticipantKindDetail::Forwarded`
  (该变体仅标记模拟测试参与者,正常场景不出现)。

- **文件**: `src/rtc_engine/mod.rs`
- **问题**: 上游代码里 `SessionEvent::SipDTMF` 的 match arm 重复出现两次 → unreachable
  pattern 告警。
- **补丁**: 删除重复的第二个 arm(与第一个完全相同,无行为变化)。

## 告警清理(仅 lint 层面,无行为变化)

livekit-api-0.5.6 / livekit-0.7.52 在本项目的 feature/platform 组合下有一批编译告警,
处理如下:

- 给仍需填充 deprecated proto 字段以兼容旧服务端的函数/语句加 `#[allow(deprecated)]`
  (`media_encryption`、`bypass_transcoding`、`play_ringtone`、`audio/video_quality`、
  `e2ee`、`disable_dtx`、`DataPacket::kind`、`TrackInfo::simulcast/layers`、
  `UserPacket::participant_sid/identity`、`RoomEvent::Stream*Received` 等)。
- 上游保留但本构建未调用的 API(音频设备管理、`TwirpClient::new`、`try_result` 等)
  加 `#[allow(dead_code)]`。
- 未使用的 import 直接删除;未使用的 `filter` 参数改名 `_filter`;
  `base64::encode` 换成 `Engine::encode`;`wait_reconnection` 返回值补上显式 `'_`
  生命周期。

## libwebrtc-0.3.43

- **文件**: `src/native/peer_connection.rs`
- **问题**: `webrtc-sys 0.3.43` 的 FFI `RtcConfiguration` 新增 `enable_sctp_snap`,
  安全封装的转换字面量未设置 → E0063。
- **补丁**: 转换处补 `enable_sctp_snap: false,`(保持旧行为)。

## 配套版本约束(Cargo.lock 已锁定)

livekit 0.7.52 + libwebrtc/webrtc-sys 0.3.43 + livekit-api 0.5.6 + livekit-protocol 0.7.12
+ livekit-common 0.1.1 + livekit-datatrack 0.1.12 + livekit-data-stream 0.1.0

注意: webrtc-sys 0.3.42 与 0.3.43 的 C++ 源码和预编译包互不兼容,不可混用;
构建需要 clang ≥ 21(`CC=clang-21 CXX=clang++-21`)。
