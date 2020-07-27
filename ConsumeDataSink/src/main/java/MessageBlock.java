

public class MessageBlock {
	
	long  InsertionTime;
	StringBuffer body;
	
	
	public MessageBlock(long l, StringBuffer body) {
		super();
		InsertionTime = l;
		this.body = body;
	}
	public long getInsertionTime() {
		return InsertionTime;
	}
	public void setInsertionTime(long insertionTime) {
		InsertionTime = insertionTime;
	}
	public StringBuffer getBody() {
		return body;
	}
	public void setBody(StringBuffer body) {
		this.body = body;
	}
	
	

}
