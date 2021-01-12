package distributed.systems.distributed.document.search.frontend.networking;

import com.google.protobuf.InvalidProtocolBufferException;

public interface OnRequestCallBack {

    byte [] handleRequest(byte[] requestPayload) throws InvalidProtocolBufferException;

    String getEndPoint();
}
