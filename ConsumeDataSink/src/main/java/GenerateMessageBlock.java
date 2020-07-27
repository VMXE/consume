import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class GenerateMessageBlock {
	
	//1 Create array buff data structure and add stream data to it 
	//1. start recurring daemon thread that runs to swap the array buffer create a new block(first interval, second,etc) and add 
		 //some data structure.
	
	// ANother daemon thread will keep pushing blocks to BlockManager(in Main method)
	
	// Another class and thread pool will read this block manager and insert into database..
	
	//Delivery will be auto-acknowledge. Write ahead log implementation future
	
private	volatile StringBuffer currentBuffer = new StringBuffer();
private BlockingQueue<MessageBlock> blocksForLoading = new ArrayBlockingQueue<>(10);
private volatile String state = "Initialized";
private final static String url = "jdbc:postgresql://localhost/databasename";
private final static String user = "guest";
private final static String password = "guest";


Runnable updateBuffer = () -> {
	
	MessageBlock newBlock = null;
	System.out.println("update buffer"+Thread.currentThread().getName()+java.time.LocalTime.now());
	synchronized(GenerateMessageBlock.class){
		System.out.println(currentBuffer.length());
	if(currentBuffer.length() != 0) {
		//System.out.println(currentBuffer);
		StringBuffer newBlockBuffer =  currentBuffer;
		currentBuffer = new StringBuffer();
		newBlock = new MessageBlock(System.currentTimeMillis(),newBlockBuffer);
		//System.out.println("1."+newBlock.getInsertionTime());
		
		
/*		try {
			File file = new File("//Users//vmxe//hackfileio//op.csv"+newBlock.getInsertionTime());
			file.getParentFile().mkdirs();
			BufferedWriter fw = new BufferedWriter(new FileWriter(file));
			fw.write(newBlock.getBody().toString());
			fw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}
	};
	
	if(newBlock != null) {
		
		try {
			blocksForLoading.put(newBlock);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	};

	Runnable c = () -> {
		System.out.println("LOAD buffer"+Thread.currentThread().getName()+java.time.LocalTime.now());
		keepLoadingData();
		
	}; 

public synchronized void addData(String data) {
	
	//System.out.println(data);
	currentBuffer.append(data);
	//System.out.println(data);
	
}

public void start() {
	System.out.println("DAEMON1 INITIALIZATION");
	ScheduledExecutorService ftses = Executors.newScheduledThreadPool(1,new ThreadFactory() {
		public Thread newThread(Runnable r) {
			
			Thread t = Executors.defaultThreadFactory().newThread(r);
			t.setDaemon(true);
			return t;
		}
		
		
	});
	
	System.out.println("DAEMON2 INITIALIZATION");
	ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
		public Thread newThread(Runnable r) {
			
			Thread t = Executors.defaultThreadFactory().newThread(r);
			t.setDaemon(true);
			return t;
		}
		
		
	});
	System.out.println("DAEMON1 STARTED");
	ftses.scheduleWithFixedDelay(updateBuffer, 1, 2,TimeUnit.SECONDS);
	System.out.println("DAEMON2 STARTED");
	executor.submit(c);
}

public GenerateMessageBlock() {
	super();
}

public void keepLoadingData() {
	
	
	try {
	while(true) {
		
		System.out.println("BLOCKS ARE LOADING");
	MessageBlock b = blocksForLoading.take();
	System.out.println(b.getBody().length());
	jdbcInsert(b);
	
		}
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		
	}
		
}

public  void jdbcInsert(MessageBlock message) {
	
	String Update_ClaimId_SQL = "UPDATE singleton_claimsv1 SET bill_amount=?,deductible_amt=? WHERE claim_id=?";
	String INSERT_ClaimId_SQL = "INSERT INTO singleton_claimsv1" + "(claim_id, original_claim_id, bill_amount, deductible_amt, approved_units, value_code1, member_clientid, provider_clientid, bill_from_dt, bill_to_dt, diag_cd1, diagcd_description, proccd_dt) VALUES " +
            " (?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?);";
	double Total_sum = 0;
	try (Connection connection = DriverManager.getConnection(url, user, password);
            // Step 2:Create a statement using connection object
			PreparedStatement preparedStatementUpdate = connection.prepareStatement(Update_ClaimId_SQL);
            PreparedStatement preparedStatementInsert = connection.prepareStatement(INSERT_ClaimId_SQL)) {
            connection.setAutoCommit(false);

            String stringMessageBlock = message.getBody().toString();
            System.out.println(stringMessageBlock);
            String[] stringMessageLine = stringMessageBlock.split(System.lineSeparator());
           
            for(String stringMessage:stringMessageLine) {
            	
            
            String[] splitMessage = stringMessage.split(",");
            
            Total_sum += Double.valueOf(splitMessage[14]);
            
            System.out.println(Total_sum);
            
            if(splitMessage[13].equalsIgnoreCase("N")) {
            	

            preparedStatementInsert.setString(1,splitMessage[0]);
            preparedStatementInsert.setString(2,splitMessage[1]);
            preparedStatementInsert.setBigDecimal(3,BigDecimal.valueOf(Double.valueOf(splitMessage[2])));
            preparedStatementInsert.setBigDecimal(4,BigDecimal.valueOf(Double.valueOf(splitMessage[3])));
            preparedStatementInsert.setString(5,splitMessage[4]);
            preparedStatementInsert.setString(6,splitMessage[5]);
            preparedStatementInsert.setString(7,splitMessage[6]);
            preparedStatementInsert.setString(8,splitMessage[7]);
            preparedStatementInsert.setTimestamp(9,Timestamp.valueOf(splitMessage[8]));
            preparedStatementInsert.setTimestamp(10,Timestamp.valueOf(splitMessage[9]));
            preparedStatementInsert.setString(11,splitMessage[10]);
            preparedStatementInsert.setString(12,splitMessage[11]);
            preparedStatementInsert.setTimestamp(13,Timestamp.valueOf(splitMessage[12]));
    
            preparedStatementInsert.addBatch();
    
            } else {
            	preparedStatementUpdate.setBigDecimal(1,BigDecimal.valueOf(Double.valueOf(splitMessage[2])));
            	preparedStatementUpdate.setBigDecimal(2,BigDecimal.valueOf(Double.valueOf(splitMessage[3])));
            	preparedStatementUpdate.setString(3,splitMessage[0]);
            	preparedStatementUpdate.addBatch();
            }
            
            
            
            
            }
	
int[] insertCounts = preparedStatementInsert.executeBatch();
int[] updateCounts = preparedStatementUpdate.executeBatch();
System.out.println(Arrays.toString(insertCounts));
            System.out.println(Arrays.toString(updateCounts));
            connection.commit();
            connection.setAutoCommit(true);
            
            
    if(Double.parseDouble(new DecimalFormat("##.####").format(Total_sum)) == 5403.28 ) {
    	
    	System.out.println("Finished Challenge: <Singleton> Total Bill Amount: 343,324.34”");
    }
            
        } catch (BatchUpdateException batchUpdateException) {
        	System.out.println(batchUpdateException);
        } catch (SQLException e) {
        	System.out.println(e);
        }
    }

}
