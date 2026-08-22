//! `UniversalBridgeMailbox` ABI bindings and calldata helpers.

use alloy::sol;

sol! {
    #[derive(Debug)]
    struct MessageHeader {
        uint256 chainSrc;
        uint256 chainDest;
        address sender;
        address receiver;
        uint256 sessionId;
        string label;
    }

    #[derive(Debug)]
    struct Message {
        MessageHeader header;
        bytes payload;
    }

    #[derive(Debug)]
    function putInbox(
        uint256 chainMessageSender,
        address sender,
        address receiver,
        uint256 sessionId,
        string label,
        bytes data
    );

    #[derive(Debug)]
    function removeInbox(
        uint256 chainMessageSender,
        address sender,
        address receiver,
        uint256 sessionId,
        string label,
        bytes data
    );

    #[derive(Debug)]
    function writeMessage(Message message);

    #[derive(Debug)]
    function readMessage(MessageHeader header) returns (bytes);

    #[derive(Debug)]
    function bridgeERC20To(uint256 chainDest, address tokenSrc, uint256 amount, address receiver, uint256 sessionId);

    #[derive(Debug)]
    function bridgeCETTo(uint256 chainDest, address cetTokenSrc, uint256 amount, address receiver, uint256 sessionId);

    #[derive(Debug)]
    function bridgeEthTo(uint256 sessionId, uint256 chainDest, address receiver);

    #[derive(Debug)]
    function receiveTokens(MessageHeader msgHeader);
    
    #[derive(Debug)]
    function receiveETH(MessageHeader msgHeader);

    #[derive(Debug)]
    function approve(address spender, uint256 amount) returns (bool);

    /// Layout of the `SEND_TOKENS` payload written by `bridgeERC20To`/`bridgeCETTo`
    /// (`abi.encode(block.chainid, tokenSrc, amount, name, symbol, decimals)`).
    /// Only the leading static fields are needed on the receiving side, and
    /// decoding a prefix of a tuple is safe: the head slots of the static
    /// fields keep their positions regardless of what dynamic fields follow.
    #[derive(Debug)]
    struct SendTokensPayloadHead {
        uint256 remoteChainID;
        address remoteAsset;
        uint256 amount;
    }

    /// Layout of the `SEND_ETH` payload written by `bridgeEthTo`
    /// (`abi.encode(block.chainid, msg.value)`).
    #[derive(Debug)]
    struct SendEthPayload {
        uint256 chainSrc;
        uint256 amount;
    }

    /// Layout of the `ACK` payload written by `receiveTokens`/`receiveETH`
    /// (`abi.encode(remoteAsset, amount)`, `remoteAsset` is `address(0)` for ETH).
    #[derive(Debug)]
    struct AckPayload {
        address remoteAsset;
        uint256 amount;
    }

    // ComposeL2ToL2Bridge finalize/compensate calls, all `onlyCoordinator`.

    #[derive(Debug)]
    function sendConfirm(MessageHeader sendHeader);

    #[derive(Debug)]
    function sendAbortToken(
        uint256 chainDest,
        address token,
        address sender,
        address receiver,
        uint256 amount,
        uint256 sessionId
    );

    #[derive(Debug)]
    function sendAbortETH(
        uint256 chainDest,
        address sender,
        address receiver,
        uint256 amount,
        uint256 sessionId
    );

    #[derive(Debug)]
    function recvConfirmToken(MessageHeader msgHeader) returns (address, uint256);

    #[derive(Debug)]
    function recvAbortToken(MessageHeader msgHeader);

    #[derive(Debug)]
    function recvConfirmETH(MessageHeader msgHeader) returns (uint256);

    #[derive(Debug)]
    function recvAbortETH(MessageHeader msgHeader);
}

pub(crate) fn calldata_selector(input: &str) -> Option<[u8; 4]> {
    let input = input.strip_prefix("0x").unwrap_or(input);
    let selector_hex = input.get(..8)?;

    let mut selector = [0u8; 4];
    hex::decode_to_slice(selector_hex, &mut selector).ok()?;
    Some(selector)
}