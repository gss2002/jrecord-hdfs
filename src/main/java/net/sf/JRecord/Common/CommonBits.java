package net.sf.JRecord.Common;

import java.nio.charset.Charset;

public class CommonBits {
	private static final String LINE_SEPARATOR = System.getProperty("line.separator");
	private static final char[] EMPTY_CHAR_ARRAY = {};
	public  static final String NULL_STRING = new String(EMPTY_CHAR_ARRAY, 0, 0); 
	public  static final Object NULL_VALUE = NULL_STRING;
	private static byte[] EBCDIC_EOL_BYTES = {0x15};
	
	/**
	 * This variable is used in JRecord to control wether CsvLines (based on a List)
	 * is used or the more normal Line / CharLine !!!
	 */
	private static boolean useCsvLine = false;
	
	
	/**
	 * Get the eol chars for a file based on the eol-description and charset
	 * @param defaultEolBytes
	 * @param eolDesc
	 * @param charset
	 * @return
	 */
	public static byte[] getEolBytes(byte[] defaultEolBytes, String eolDesc, String charset) {
		byte[] recordSep = defaultEolBytes;
		
	    if (Constants.CRLF_STRING.equals(eolDesc)) {
	        recordSep = Constants.CRLF_BYTES;
	    } else if (defaultEolBytes == null || Constants.DEFAULT_STRING.equals(eolDesc) || "".equals(eolDesc)) {
	    	recordSep = Constants.SYSTEM_EOL_BYTES;
	    } else if (Constants.CR_STRING.equals(eolDesc)) {
	        recordSep = Constants.CR_BYTES;
	    } else if (Constants.LF_STRING.equals(eolDesc)) {
	        recordSep = Constants.LF_BYTES;
	    } 
	    if (charset != null && (! "".equals(charset)) && Charset.isSupported(charset)) {
			try {
				byte[] newLineBytes = Conversion.getBytes("\n", charset);
				if (newLineBytes.length == 1 && newLineBytes[0] == EBCDIC_EOL_BYTES[0]) {
		        	recordSep = EBCDIC_EOL_BYTES;
		        } else if (Constants.CRLF_STRING.equals(eolDesc)) {
			        recordSep = Conversion.getBytes("\r\n", charset);
			    } else if (eolDesc == null || Constants.DEFAULT_STRING.equals(eolDesc) || "".equals(eolDesc) ) {
			    	recordSep = Conversion.getBytes(LINE_SEPARATOR, charset);
			    } else if (Constants.CR_STRING.equals(eolDesc)) {
			        recordSep = newLineBytes;
			    } else if (Constants.LF_STRING.equals(eolDesc)) {
			        recordSep = Conversion.getBytes("\r", charset);
			    } else if (defaultEolBytes == null) {
			    	recordSep = newLineBytes;
			    }
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return recordSep;
	}
	
	/**
	 * Get the end-of-line string from the eol-description
	 * @param eolDesc eol-description
	 * @param charset charset being used
	 * @return eol-String
	 */
	public static String getEolString(String eolDesc, String charset) {
		String recordSep = eolDesc;
		
	    if (Constants.CRLF_STRING.equals(eolDesc)) {
	        recordSep = "\r\n";
	    } else if (eolDesc == null || Constants.DEFAULT_STRING.equals(eolDesc) || "".equals(eolDesc)) {
	    	recordSep = LINE_SEPARATOR;
	    } else if (Constants.CR_STRING.equals(eolDesc)) {
	        recordSep = "\n";
	    } else if (Constants.LF_STRING.equals(eolDesc)) {
	        recordSep = "\r";
	    } 
	    if (charset != null && (! "".equals(charset)) && Charset.isSupported(charset)) {
			try {
				byte[] newLineBytes = Conversion.getBytes("\n", charset);
				if (newLineBytes.length == 1 && newLineBytes[0] == EBCDIC_EOL_BYTES[0]) {
		        	recordSep = "\n";
		        } 
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return recordSep;
	}

	public static final boolean useCsvLine() {
		return useCsvLine;
	}

	public static final void setUseCsvLine(boolean useCsvLine) {
		CommonBits.useCsvLine = useCsvLine;
	}

}
