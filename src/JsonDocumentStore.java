import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
/*
class JsonDocumentStore is the main class where the LinkedHashMap of String(document from standard input as single string) as key
and Map <String, Object> as value is created from the standard input. 
Method handleInput() goes through every input line adds, gets or deletes the documents by querying the Document Store which is
a LinkedHashMap as mentioned. 
Use of LinkedHashMap preserves preserves the insertion order as well.
*/
public class JsonDocumentStore
{
	public static void main(String args[])
	{
		LinkedHashMap<String, Map<String, Object>> map = new LinkedHashMap<String, Map<String, Object>>();
		map = handleInput();
		/*
		for(Entry<String, Map<String, Object>> entry : map.entrySet())
		{
			System.out.println("Map key is " +entry.getKey());
			Map<String, Object> jsonObject = entry.getValue();
			System.out.println(" Map value "+jsonObject.toString());
		}	
		*/	
	}
	/*
	 *  read input from command line and return the final LinkedHashMap where 
	 *  keys are each command as string and values are Map Objects
	 */
	public static LinkedHashMap<String, Map<String, Object>> handleInput() 
	{
		Scanner stdin = new Scanner(System.in);
		String line;
		LinkedHashMap<String, Map<String, Object>> commandsMap = new LinkedHashMap<String, Map<String, Object>>();
		//ConcurrentMap<String, Map<String, Object>> concurrentMap = new ConcurrentHashMap<> (commandsMap);
        while(stdin.hasNextLine()&&!( line = stdin.nextLine() ).equals( "" ))
        {
        	//System.out.println("input is " +line);
        	String type = line.substring(0, line.indexOf(' '));
        	String command = line.substring(line.indexOf(' ') + 1);
        	if(type.trim().equalsIgnoreCase("add"))
        	{
        		commandsMap = addRecord(commandsMap, command);
        	}
        	else if(type.trim().equalsIgnoreCase("get"))
        	{
        		getRecord(commandsMap, command);
        	}
        	else if(type.trim().equalsIgnoreCase("delete"))
        	{
        		deleteRecord(commandsMap, command);
        	}
        	else
        	{
        		break;
        	}
        }
        stdin.close();
       
        return commandsMap;
	}
	
	// adding record in the LinkedHashMap where key is the record string itself and value is Object
	synchronized public static LinkedHashMap<String, Map<String, Object>> addRecord(LinkedHashMap<String, Map<String, Object>> commandsMap, String command)
	{
		JsonParserUtility jsonParser = new JsonParserUtility(command);
		Map<String, Object> jsonObject = jsonParser.getJSONObject();
		commandsMap.put(command, jsonObject);
		return commandsMap;
	}
	
	/*
     method getRecord() takes 2 input parameters
     1. LinkedHashMap : commandsMap which is populated with the add commands so far
     2. add command specifying the Query parameters
     Since, the search in the Map of deeply nested JSON objects can be highly expensive when it comes to
     complex and ggantic json structures as well as query parameters,
     the getRecord() method creates the JSON object from the string command
     and then flattens it to make the HashMap inside the get command JSON object single hierarchical
     3. Similarly, it flattens the each Map added to the document store ie.e LinkedHashMap so far and 
     checks whether the get commans JSON object is preset inside the bigger JSON for every LinkedHashMap value i.e. JSON Object
    */
	synchronized public static LinkedHashMap<String, Map<String, Object>> getRecord(LinkedHashMap<String, Map<String, Object>> commandsMap, String command)
	{
		JsonParserUtility jsonParser = new JsonParserUtility(command);
		Map<String, Object> jsonObject = jsonParser.getJSONObject();
		FlattenHashMap obj = new FlattenHashMap();
		jsonObject = obj.flattenMap(jsonObject);

		synchronized(commandsMap)
		{
            for(Map.Entry<String, Map<String, Object>> entry:commandsMap.entrySet())
            {
                Map<String, Object> temp = entry.getValue();
                if(obj.flattenMap(temp).entrySet().containsAll(jsonObject.entrySet()))
                {
                    System.out.println(entry.getKey());
                }
            }
		}
		return commandsMap;
	}
	
    /*
        Similar to getRecord(), deleting the record also involves creating the flatten hashmap objects of delete command 
        as well as input documents added in the store so far, and based on the complete match of entrySet() from delete object,
        deleting the corresponding documents from the LinkedHashMap
    */
	synchronized public static LinkedHashMap<String, Map<String, Object>> deleteRecord(LinkedHashMap<String, Map<String, Object>>                                                                                                  commandsMap, String command)
	{
		if(command == "" || commandsMap == null)
			return null;
        
		JsonParserUtility jsonParser = new JsonParserUtility(command);
		Map<String, Object> jsonObject = jsonParser.getJSONObject();
		FlattenHashMap obj = new FlattenHashMap();
		jsonObject = obj.flattenMap(jsonObject);
		Iterator it = commandsMap.keySet().iterator();
		while (it.hasNext())
		   {
		      String key = (String) it.next();
		      Map<String, Object> value = commandsMap.get(key);
		      
		      if (obj.flattenMap(value).entrySet().containsAll(jsonObject.entrySet()))
		      {
		    	  it.remove();
		      }
		   }
        
		return commandsMap;
	}
}

/*
 class JsonParseUtility provides the custom implementation of JSON like object
 where the parameters used are Map<String, Object> to store the key as tag name and
 value as the Object. The structure can be nested depending on the input string. 
 The core of this class is creating the character array of string which is passed
 and identify the every character as well as special characters that are especially used in creating
 JSON structure. Recursive implementation of Object creation for supporting the string with nested tags.
*/

class JsonParserUtility 
{
		final char[] arr;
		int index;
		Map<String, Object> jsonObject = null;
		public JsonParserUtility(final String jsonString)
		{
			// converting string to character array
			this.arr = jsonString.toCharArray();
			// initializing start position
			this.index = 0;
		}
    
		// filling HashMap where key if the JSON string and value if the JSON Object
		public Map<String, Object> getJSONObject() 
		{
			if (jsonObject == null) 
			{
				jsonObject = parseJSON();
			}
			return jsonObject;
		}
        // parsing string as per JSON structure and creating Objects to add in the HashMap
		private Map<String, Object> parseJSON() 
		{
			final Map<String, Object> jsonObject = new HashMap<String, Object>();
			ignoreWhiteSpaces();
            // ignoring the start of theObject which is '{'
			ignoreCharacter('{');
			while (arr[index] != '}')
			{
                // ignoring the ',' which separates tag - value pairs
				ignoreCharacter(',');
				final String key = parseString();
				
                 // ignoring the ':' which separates a tag and a value inside a pair 
				ignoreCharacter(':');
				final Object value = parseValue();
				
				jsonObject.put(key, value);
				ignoreCharacter(',');
				//System.out.println(index);
			}
			ignoreCharacter('}');
			return jsonObject;
		}
		
        // storing the value based on the different data types & also taking care of the nested tags by calling parseJSON() if
        // '{' is observed 
		private Object parseValue() 
		{
			ignoreWhiteSpaces();
			final char c = arr[index];
			if (c == '"') return parseString();
			if (c == '-' || c == '+' || Character.isDigit(c)) return parseNumber();
			if (c == '{') return parseJSON();
			if (c == 't') return parseConstant("true", Boolean.TRUE);
			if (c == 'f') return parseConstant("false", Boolean.FALSE);
			if (c == '[') return parseList();
			return parseConstant("null", null);
		}
		
        // Parsing the List of numbers
		public Object parseList()
		{
			List<Integer> list = new ArrayList<Integer>();
			ignoreCharacter('[');
			while (index<arr.length ) 
			{
				if(arr[index] == ']')
				{
					index++;
					break;
				}
				ignoreCharacter(',');
				ignoreWhiteSpaces();
				boolean positive = true;
				ignoreCharacter('"');
				
				if(arr[index] == '-')
				{
					positive = false;
				}
				
				if(Character.isDigit(arr[index]))
				{
					StringBuilder sb = new StringBuilder();
					while(Character.isDigit(arr[index]))
					{
						sb.append(arr[index]);
						index++;
					}
					
					int number = Integer.parseInt(sb.toString());
					//System.out.println(number);
					//System.out.println(index);
					if(!positive)
					{
						number = -1*number;
					}
					list.add(number);
				}
			}
			
			return list;
		}
		
        // parsing the constants
		private Object parseConstant(String name, Object object) 
		{
			ignoreWhiteSpaces();
			for (char c : name.toCharArray()) 
			{
				ignoreCharacter(c);
			}
			return object;
		}
        
        // parsing the number
		private Integer parseNumber() 
		{
            StringBuilder sb = new StringBuilder();
			int sign = +1;
			int number = 0;
			ignoreCharacter('+');
			if (arr[index] == '-') 
			{
				ignoreCharacter('-');
				sign = -1;
			}
			while (Character.isDigit(arr[index])) 
			{
				sb.append(arr[index++]);
            }
            
			return sign * Integer.parseInt(sb.toString());
		}
        
        // parsing the string
		private String parseString() 
		{
			if(arr[index] == '}')
				return null;
			
			final StringBuilder sb = new StringBuilder();
			ignoreCharacter('"');
			while (arr[index] != '"' && index < arr.length) 
			{
				sb.append(arr[index++]);
			}
			ignoreCharacter('"');
			
			return sb.toString();
		}
        
        // incrementing index by igniring given character
		private void ignoreCharacter(char ch) 
		{
			ignoreWhiteSpaces();
			if (arr[index] == ch) {
				index++; // ckip ch
				ignoreWhiteSpaces();
			}
		}
		
        // incrementing index by ignoring white spaces
		private boolean ignoreWhiteSpaces() 
		{
			while (index < arr.length && Character.isWhitespace(arr[index])) 
			{
				index++;
			}
			
			return index < arr.length;
		}
}

/*
class FlattenHashMap is used to create a HashMap of single hierarchy key values 
and remove the nested structure from the HashMap representation
*/

class FlattenHashMap
{
	public FlattenHashMap()
	{ 
		
	}

 public Map<String, Object> flattenMap(Map<String, Object> map)
 {
	 Map<String, Object> flatMap = new HashMap<>();
	 Iterator iterator = map.entrySet().iterator();
	 
	 while (iterator.hasNext()) 
	 {
		 Map.Entry<String, Object> entry = (Map.Entry<String, Object>)iterator.next();
		 String key = entry.getKey();
		 Object val = entry.getValue();
		 
		 if(val instanceof Map)
		 {
			 flatMap.putAll((Map<String, Object>)val);
		 }
		 else if(val instanceof Integer)
		 {
			 val = String.valueOf(val);
			 flatMap.put(key, val.toString());
		 }
		 else if(val == Boolean.FALSE) 
		 {
			 val = String.valueOf(Boolean.FALSE).toString();
			 flatMap.put(key, val);
		 } 
		 else if(val == Boolean.TRUE) 
		 {
			 val = String.valueOf(Boolean.TRUE).toString();
			 flatMap.put(key, val);
		 } 
		 else if(val instanceof List)
		 {
			 for(Object item : (List<String>)val)
			 {
				 if (item instanceof Map) 
				 {
					 flatMap.putAll((Map<String, Object>) item);
				 } 
				 else
				 {
					 flatMap.put(item.toString(), item);
				 }
			 }
		 }
		 else
		 {
			 flatMap.put(key, val.toString());
		 }
	 }
	 
	 return flatMap;
 }
 
}
