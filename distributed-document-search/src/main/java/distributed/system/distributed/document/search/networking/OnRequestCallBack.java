package distributed.system.distributed.document.search.networking;

import com.google.protobuf.InvalidProtocolBufferException;

public interface OnRequestCallBack {

    byte [] handleRequest(byte[] requestPayload) throws InvalidProtocolBufferException;

    String getEndPoint();
}
