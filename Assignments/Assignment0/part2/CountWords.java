package Assignment0;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CountWords {

    public static void main(String[] args) {
        String url = "http://www.gutenberg.org/files/98/98-0.txt";
        HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
        
        try {
            downloadUsingNIO(url, "input.txt");
            
         
            String fileName = "input.txt";
            String line = null;

            try {
               
                FileReader fileReader = 
                    new FileReader(fileName);

                
                BufferedReader bufferedReader = 
                    new BufferedReader(fileReader);

                while((line = bufferedReader.readLine()) != null) {
                	Pattern p = Pattern.compile("[\\w']+");
                	
           		 Matcher m = p.matcher(line);
           		
           		 ArrayList<String> words=new ArrayList<String>();
           		 while ( m.find() ) {
           		 words.add(line.substring(m.start(), m.end()));
           		 }
           		
           		 for (String word : words) {
           			 word=word.toLowerCase().trim();
           			word= word.replaceAll("_" ,"");
           	
           			if(!word.isEmpty()&& !isNumber(word)){
           		 if (wordCount.containsKey(word)) {
           		 wordCount.put(word, wordCount.get(word) + 1);
           		 }
           		
           		 else {
           		 wordCount.put(word, 1);
           		 }
           		 }}
                } 
                Set<Entry<String, Integer>> entrySet = wordCount.entrySet();

        		List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(entrySet);

        		Collections.sort(list, new Comparator<Entry<String, Integer>>() {
        			@Override
        			public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
        				
        				if(e2.getValue()>e1.getValue()){
        				   return 1;
        				}
        				else if(e2.getValue()<e1.getValue()){
        					return -1;
        				}
        				else {
        					return e2.getKey().compareTo(e1.getKey());
        				}
        				
        			}
        		});
        		
        		File file = new File("sxm155431Part2.txt");
        		BufferedWriter output = null;
        		output = new BufferedWriter(new FileWriter(file));
 
        		for (Entry<String, Integer> entry : list) {

        			output.write((entry.getKey()+" "+entry.getValue()));
                    
        			output.newLine();
        		}
        
        		
        		output.close();

           
                bufferedReader.close();         
            }
            catch(FileNotFoundException ex) {
                System.out.println(
                    "Unable to open file '" + 
                    fileName + "'");                
            }
            catch(IOException ex) {
                System.out.println(
                    "Error reading file '" 
                    + fileName + "'");                  
               
            }
        }
    
    

            
         
         catch (IOException e) {
            e.printStackTrace();
        }
        
        
        
    }

  

    private static void downloadUsingNIO(String urlStr, String file) throws IOException {
        URL url = new URL(urlStr);
        ReadableByteChannel rbc = Channels.newChannel(url.openStream());
        FileOutputStream fos = new FileOutputStream(file);
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        fos.close();
        rbc.close();
    }
    
    public static boolean isNumber(String str)  
    {  
      try  
      {  
        double d = Double.parseDouble(str);  
      }  
      catch(NumberFormatException nfe)  
      {  
        return false;  
      }  
      return true;  
    }

}