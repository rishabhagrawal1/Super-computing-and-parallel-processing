import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class InputHelper {

	  static long wIns(long i, long j)
	  {
		  long ret = j*j - i*i;
		  return ret;
	  }
	  
	  static long wDel(long i, long j)
	  {
		  long ret = j*j*j - i*i*i;
		  return ret;
	  }
	  
	  static int sij(char a, char b)
	  {
		  if(a == b)
			  return 0;
		  return 1;
	  }
	  
	  private static String readFile(String pathname) throws IOException {

		    File file = new File(pathname);
		    StringBuilder fileContents = new StringBuilder((int)file.length());
		    Scanner scanner = new Scanner(file);
		    //String lineSeparator = System.getProperty("line.separator");

		    try {
		        while(scanner.hasNextLine()) {
		            fileContents.append(scanner.nextLine());// + lineSeparator);
		        }
		        return fileContents.toString();
		    } finally {
		        scanner.close();
		    }
		}
	  
	  public static void CheckInputDPTablesLoop4(long m, long n, long[][] dpTable, String pathname1, String pathname2) throws IOException
	  {
		  BufferedWriter output1 = null;
		  String s1 = readFile(pathname1);
		  s1 = "~"+s1;
		  String s2 = readFile(pathname2);
		  s2 = "~"+s2;
		  long min = 0;
	      try {
	          File file1 = new File("inputCheckDPLoop4.txt");
	          output1 = new BufferedWriter(new FileWriter(file1));
			  String s = "";
    		  output1.write(s);
    		  int j = 0;
    		  int i = 0;
    		  int t = 1;
	          for(; t <= (m + n - 1); t++){
	        	  for(i = Math.max(1, t+1-(int)n); i <= Math.min(t, m); i++){
		        		j = t+1-i;  
		        		if(i < m && j < n)
		        		{
		        			dpTable[i+1][j+1] = Math.min(dpTable[i+1][j+1], dpTable[i][j] + sij(s1.charAt(i+1),s2.charAt(j+1)));
		        		}
		        		for(int q = j+1; q <=n; q++)
		        		{
		        			dpTable[i][q] = Math.min(dpTable[i][q],dpTable[i][j] + wIns(j,q));
		        		}
		        		for(int p = i+1; p <=m; p++)
		        		{
		        			dpTable[p][j] = Math.min(dpTable[p][j],dpTable[i][j] + wIns(i,p));
		        		}
	        	  }
	          }
	          for(i = 0; i <= m; i++)
	          {
	        	  for(j = 0; j <= n; j++)
		          {
	            		s = ""+(i*(n+1)+j)+" "+ dpTable[i][j]+"\n";
	              		output1.write(s);
		          }  
	          }

	      } catch ( IOException e ) {
	          e.printStackTrace();
	      } finally {
	        if ( output1 != null ) {
	          output1.close();
	        }
	      }
	  }
	  
	  public static void CheckInputDPTablesLoop3(long m, long n, long[][] dpTable, String pathname1, String pathname2) throws IOException
	  {
		  BufferedWriter output1 = null;
		  String s1 = readFile(pathname1);
		  s1 = "~"+s1;
		  String s2 = readFile(pathname2);
		  s2 = "~"+s2;
		  long min = 0;
	      try {
	          File file1 = new File("inputCheckDPLoop3.txt");
	          output1 = new BufferedWriter(new FileWriter(file1));
			  long a = 0;
			  long b = 0; 
			  String s = "";
    		  output1.write(s);
	          for(int j = 1; j <= n; j++){
	        		  a = dpTable[1][j];
	        		  b = dpTable[0][j-1] + sij(s1.charAt(1),s2.charAt(j));
	        		  min = Math.min(a, b);
	        		  s = ""+(1*(n+1)+j)+" "+ min+"\n";
	        		  dpTable[1][j] = min;
	        		  output1.write(s);
	          }
	          CheckInputDPTablesLoop4(m, n, dpTable, pathname1, pathname2);
	      } catch ( IOException e ) {
	          e.printStackTrace();
	      } finally {
	        if ( output1 != null ) {
	          output1.close();
	        }
	      }
	  }
	  
	  public static void CheckInputDPTablesLoop2(long m, long n, long[][] dpTable, String pathname1, String pathname2) throws IOException
	  {
		  System.out.print("In CheckInputDPTablesLoop2");
          for(int i = 0; i <= m; i++)
          {
        	  System.out.print("\n");
        	  for(int j = 0; j <= n; j++)
	          {
            		System.out.print(dpTable[i][j]);
	          }  
          }  
		  BufferedWriter output1 = null;
		  String s1 = readFile(pathname1);
		  s1 = "~"+s1;
		  System.out.println(s1);
		  String s2 = readFile(pathname2);
		  s2 = "~"+s2;
		  long min = 0;
	      try {
	          File file1 = new File("inputCheckDPLoop2.txt");
	          output1 = new BufferedWriter(new FileWriter(file1));
			  long a = 0;
			  long b = 0; 
			  String s = "";
    		  output1.write(s);
	          for(int i = 1; i <= m; i++){
	        		  a = dpTable[i][1];
	        		  b = dpTable[i-1][0] + sij(s1.charAt(i),s2.charAt(1));
	        		  min = Math.min(a, b);
	        		  s = ""+(i*(n+1) + 1)+" "+ min+"\n";
	        		  dpTable[i][1] = min;
	        		  output1.write(s);
	          }
	          CheckInputDPTablesLoop3(m, n, dpTable, pathname1, pathname2);
	      } catch ( IOException e ) {
	          e.printStackTrace();
	      } finally {
	        if ( output1 != null ) {
	          output1.close();
	        }
	      }
	  }
	  
	  public static void createCheckInputDPTables(long m, long n, String pathname1, String pathname2) throws IOException
	  {
		  BufferedWriter output1 = null;
		  long[][] dpTable = new long[(int) m+1][(int) n+1];
	      try {
	          File file1 = new File("inputCheckDPLoop1.txt");
	          output1 = new BufferedWriter(new FileWriter(file1));
			  long a = 0;
			  long b = 0;
			  long min = 0;
			  String s = "";
    		  output1.write(s);
	          for(long i = 0; i <= m; i++){
	        	  for(long j = 0; j <= n; j++){
	        		  a = wIns(0, i) + wDel(0, j);
	        		  b = wDel(0, j) + wIns(0, i);
	        		  min = Math.min(a, b);
	        		  s = ""+(i*(n+1) + j)+" "+ min +"\n";
	        		  dpTable[(int) i][(int) j] = min;
	        		  output1.write(s);
	        	  }
	          }
	          System.out.print("In CheckInputDPTablesLoop1");
	          for(int i = 0; i <= m; i++)
	          {
	        	  System.out.print("\n");
	        	  for(int j = 0; j <= n; j++)
		          {
	            		System.out.print(dpTable[i][j]);
		          }  
	          }  
	          System.out.print("\n");
	          CheckInputDPTablesLoop2(m, n, dpTable, pathname1, pathname2);
	      } catch ( IOException e ) {
	          e.printStackTrace();
	      } finally {
	        if ( output1 != null ) {
	          output1.close();
	        }
	      }
	  }
	  
	  public static void createInputDPTables(long m, long n, String pathname1, String pathname2) throws IOException
	  {
		  BufferedWriter output1 = null;
		  String s1 = readFile(pathname1);
		  String s2 = readFile(pathname2);
		  BufferedWriter output2 = null;
	      try {
	          File file1 = new File("inputDP.txt");
	          output1 = new BufferedWriter(new FileWriter(file1));
			  long a = 0;
			  long b = 0; 
			  String s = "";
			  s = ""+0+" "+0+" "+s1+" "+s2+"\n";
    		  output1.write(s);
	          for(long i = 1; i <= m; i++){
	        	  a = wIns(0, i);
	    		  s = ""+(i*(n+1))+" "+a+" "+s1+" "+s2+"\n";
	    		  output1.write(s);
	          }
	          for(long j = 1; j <= n; j++){
	        	  b = wDel(0, j);
	    		  s = ""+(j)+" "+b+" "+s1+" "+s2+"\n";
	    		  output1.write(s);        	  
	          }
	      } catch ( IOException e ) {
	          e.printStackTrace();
	      } finally {
	        if ( output1 != null ) {
	          output1.close();
	        }
	      }
	  }
	  
	  private static int getRandomNumber()
	  {
		  Random r = new Random();
		  int Low = 65;
		  int High = 90;
		  int n = r.nextInt(High-Low) + Low;
		  return n;
	  }
	  
	  public static void createInputTextStrings(long m, long n, String pathname1, String pathname2) throws IOException
	  {
		  BufferedWriter output1 = null;
		  BufferedWriter output2 = null;
	      try {
	          File file1 = new File(pathname1);
	          File file2 = new File(pathname2);
	          output1 = new BufferedWriter(new FileWriter(file1));
	          output2 = new BufferedWriter(new FileWriter(file2));
	          for(long i = 0; i < m; i++)
	        	  output1.write(Character.toChars(getRandomNumber()));
	          for(long i = 0; i < n; i++)
	        	  output2.write(Character.toChars(getRandomNumber()));
	      } catch ( IOException e ) {
	          e.printStackTrace();
	      } finally {
	        if ( output1 != null ) {
	          output1.close();
	        }
	        if ( output2 != null ) {
	            output2.close();
	          }
	      }
	  }
	  
	  public static void runHelper(long m, long n, String pathname1, String pathname2) throws Exception {
			//createInputTextStrings(m, n, pathname1, pathname2);
			//createInputDPTables(m, n, pathname1, pathname2);
			createCheckInputDPTables(m, n, pathname1, pathname2);
	  }
	  
	  public static void main(String[] args) throws Exception {
			//createInputTextStrings(Long.parseLong(args[0]), Long.parseLong(args[1]), args[2], args[3]);
			//createInputDPTables(Long.parseLong(args[0]), Long.parseLong(args[1]), args[2], args[3]);
			createCheckInputDPTables(Long.parseLong(args[0]), Long.parseLong(args[1]), args[2], args[3]);
	  }
}
