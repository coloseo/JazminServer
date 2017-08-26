package jazmin.server.protobuf;

/**
 * Created by ging on 25/08/2017.
 * JazminServer
 */

public class ProtobufMessage {

    private int id;

    private int length;

    private byte[] data;

    public ProtobufMessage(int id, int length, byte[] data) {
        this.id = id;
        this.length = length;
        this.data = data;
    }

    public int getId() {
        return id;
    }

    public int getLength() {
        return length;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return String.format("[id=%d, length=%d, data=.....]", this.getId(), this.getLength());
    }
}
