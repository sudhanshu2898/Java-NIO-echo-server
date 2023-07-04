package logging.service.core;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class Utility {

    public String ByteBufferToString(ByteBuffer buffer, Charset charset){
        byte[] bytes;
        if(buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        }
        return new String(bytes, charset);
    }

    public ByteBuffer StringToByteBuffer(String message, Charset charset){
        return ByteBuffer.wrap(message.getBytes(charset));
    }

}
