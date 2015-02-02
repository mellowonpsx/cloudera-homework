package test;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class TestStringRegEx
{
	public static void main(String args[])
	{
		String input= " 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] \"GET /cat.jpg HTTP/1.1\" 200 12433";
		String line = input.toString();
		
		Pattern ipPattern = Pattern.compile("(\\A|\\s)(?:(?:25[0-5]|2[0-4]\\d|[01]?\\d{1,2})\\.){3}(?:25[0-5]|2[0-4]\\d|[01]?\\d{1,2})(\\s|\\z)");
		// test -> http://regexpal.com/
		// testset
		// X 423.1.0.0.1 
		// V 192.168.1.20 
		// V 255.255.255.255
		// V 0.0.0.0
		// X 0.0.0.0.0
		// X 3.9.0.0.9
		// V1 ([0-2]?\d{1,2}\.){3}([0-2]?\d{1,2}) //299 is accepted
		// V2 (?:(?:25[0-5]|2[0-4]\d|[01]?\d{1,2})\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d{1,2})
		// V3->internet tips
		// \\A(?:(?:25[0-5]|2[0-4]\\d|[01]?\\d{1,2})\\.){3}(?:25[0-5]|2[0-4]\\d|[01]?\\d{1,2})(\\s|\\z)");
		Pattern monthPattern = Pattern.compile("(?<=\\[(([0-2]?\\d)|([3][0-2]))/)([A-Z][a-z]{2})(?=/(([1][9]{2}\\d)|([2][0][01]\\d)).*\\])");
		// data format: [24/Apr/2011:04:20:11 -0400]
		//play with regex is funny but wth!
		
		Matcher ip = ipPattern.matcher(line);
		Matcher month = monthPattern.matcher(line);

		if(ip.find() && month.find())
        {
			String key = ip.group().trim();
			String value = month.group().toUpperCase().trim();
			System.out.println("key: "+key+"\tvalue: "+value);
        }
		else
		{
			//add wrong line counter
			System.out.println("wrong line!");
		}
	}
}
