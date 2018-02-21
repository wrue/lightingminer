package lightingminer;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateLog {
	public static Logger logger = LoggerFactory.getLogger("genlog");
	
	public static void main(String[] args) 
	{
		for(int m = 0; m < 500; m++)
		{
			String[] logChars = new String[]{"我","哈","啊哈哈哈","hlfalsldqwepr","入","if","为发"};
			
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i < 100; i++) 
			{
				Random random = new Random();
				int r = random.nextInt(100);
				int l =	r%logChars.length;
				builder.append(logChars[l]);
			}
				
			logger.info(builder.toString());
		}
		
	}
}
