/*
 * @Author Bruce Martin
 * Created on 13/04/2007
 *
 * Purpose:
 */
package net.sf.JRecord.CsvParser;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Basic CSV line parser. Basically
 *
 *   - If the Field start with {Quote}; the fields is ended by {Quote}{Field-Seperator}
 *   - Otherwise the field ends with a {Field-Seperator}
 *
 * @author Bruce Martin
 *
 */
public final class BasicCsvLineParser extends BaseCsvLineParser implements ICsvLineParser {

    private static BasicCsvLineParser instance = new BasicCsvLineParser(false);
	public final int delimiterOrganisation;


    public BasicCsvLineParser(boolean quoteInColumnNames) {
    	this(quoteInColumnNames, ICsvDefinition.NORMAL_SPLIT, false);
    }



	public BasicCsvLineParser(boolean quoteInColumnNames, int delimiterOrganisation) {
		this(quoteInColumnNames, delimiterOrganisation, false);
	}


	public BasicCsvLineParser(boolean quoteInColumnNames, int delimiterOrganisation, boolean allowReturnInFields) {
		super(quoteInColumnNames, allowReturnInFields);
		this.delimiterOrganisation = delimiterOrganisation;
	}


	/**
     * Get the field Count
     *
     * @param line line to inspect
	 * @param lineDef Csv Definition
     * @return the number of fields
     */
    public int getFieldCount(String line, ICsvDefinition lineDef) {
        String[] fields = split(line, lineDef, 0);

        if (fields == null) {
            return 0;
        }

        return fields.length;
    }

    /**
     * Get a specific field from a line
     *
     * @see ICsvLineParser#getField(int, String, String, String)
     */
    public String getField(int fieldNumber, String line, ICsvDefinition lineDef) {
        String[] fields = split(line, lineDef, fieldNumber);

        if (fields == null  || fields.length <= fieldNumber || fields[fieldNumber] == null) {
            return null;
        }
 //       String quote = lineDef.getQuote();

        update4quote(fields, fieldNumber, lineDef.getQuote());
//        if (isQuote(quote)
//        && fields[fieldNumber].startsWith(quote)
//        && fields[fieldNumber].endsWith(quote)) {
//        	String v = "";
//
//        	if (fields[fieldNumber].length() >= quote.length() * 2) {
//	            int quoteLength = quote.length();
//				v = fields[fieldNumber].substring(
//						quoteLength, fields[fieldNumber].length() - quoteLength
//				);
//	        }
//        	fields[fieldNumber] = v;
//        }

        return fields[fieldNumber];
    }
    
    

    @Override
	public List<String> getFieldList(String line, ICsvDefinition csvDefinition) {
    	String[] fields = split(line, csvDefinition, 0);
    	if (fields == null) {
            return new ArrayList<String>(1);
        }
		String quote = csvDefinition.getQuote();
		ArrayList<String> ret =  new ArrayList<String>(fields.length);
		for (int i = 0; i < fields.length; i++) {
			update4quote(fields, i, quote);
			ret.add(fields[i]);
		}
		
		return ret;
	}


    private void update4quote(String[] fields, int fieldNumber, String quote ) {
        if (isQuote(quote)
        && fields[fieldNumber].startsWith(quote)
        && fields[fieldNumber].endsWith(quote)) {
        	String v = "";

        	if (fields[fieldNumber].length() >= quote.length() * 2) {
	            int quoteLength = quote.length();
				v = fields[fieldNumber].substring(
						quoteLength, fields[fieldNumber].length() - quoteLength
				);
	        }
        	fields[fieldNumber] = v;
        }

    }

	/**
     * @see ICsvLineParser#setField(int, int, String, String, String, String)
     */
    public final String setField(int fieldNumber, int fieldType, String line, ICsvDefinition lineDef,
            String newValue) {

        String[] fields = split(line, lineDef, fieldNumber);

        if (fields == null || fields.length == 0) {
            fields = initArray(fieldNumber + 1);
        }

        fields[fieldNumber] = formatField(newValue, fieldType, lineDef);
        
        return formatFieldArray(fields, lineDef);
    }
    
    protected String formatField(String s, int fieldType, ICsvDefinition lineDef) {
    	String quote = lineDef.getQuote();
    	if (s == null) {
    		s = "";
    	} else if  (quote != null && quote.length() > 0
		        && (   s.indexOf(lineDef.getDelimiter()) >= 0
		//        	|| s.indexOf(quote) >= 0
		        	|| s.startsWith(quote)
		        	|| s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0)) {
            s = quote + s + quote;
        }
        return s;
    }
    /**
     * Initialise array
     * @param count array size
     * @return initialised array
     */
    private String[] initArray(int count) {
        String[] ret = new String[count];

        for (int i = 0; i < count; i++) {
            ret[i] = "";
        }

        return ret;
    }


	/**
	 * Split a supplied line into its Fields. This only applies to
	 * Comma / Tab seperated files
	 *
	 * @param line line to be split
	 * @param lineDefinition Csv Definition
	 * @param min minimum number of elements in the array
	 *
	 * @return Array of fields
	 */
	public final String[] split(String line, ICsvDefinition lineDefinition, int min) {

		if ((lineDefinition.getDelimiter() == null || line == null)
		||  ("".equals(lineDefinition.getDelimiter()))) {
			return null;
		}

		int i = 0;
		StringTokenizer tok;
		int len, newLength, j;
		String[] temp, ret;
		boolean keep = true;
		String quote = lineDefinition.getQuote();

		tok = new StringTokenizer(line, lineDefinition.getDelimiter(), true);
		len = tok.countTokens();
		temp = new String[Math.max(len, min)];

		//if (min == 4) System.out.println("}}"+ quote + "< >" + isQuote(quote)+ "{{ ");
		if (! isQuote(quote)) {
		    while (tok.hasMoreElements()) {
		        temp[i] = tok.nextToken();
//		        if (min == 4) System.out.print("->>" + (i) + " " + keep + " >" + temp[i]
//		                        + "< >" + delimiter + "< ");
		        if (lineDefinition.getDelimiter().equals(temp[i])) {
		            if (keep) {
		                temp[i++] = "";
		               // if (min == 4) System.out.print(" clear ");
		            }
		            keep = true;
		        } else {
		            keep = false;
		            i += 1;
		        }
		        //if (min == 4) System.out.println(" >> "  + keep);
		    }
		    if (i < temp.length) {
		    	temp[i] = "";
		    }
		} else {
		    StringBuffer buf = null;
		    String s;
		    boolean building = false;
		    while (tok.hasMoreElements()) {
		        s = tok.nextToken();
		        if (building) {
		            buf.append(s);
		            if (s.endsWith(quote)) {
		                //buf.delete(buf.length() - 1, buf.length());
		                temp[i++] = buf.toString();
		                building = false;
		                //buf.delete(0, buf.length());
		                keep = false;
		            }
		        } else if (lineDefinition.getDelimiter().equals(s)) {
		            if (keep) {
		                temp[i++] = "";
		            }
		            keep = true;
		        } else if (s.startsWith(quote)
		        	   && (! s.endsWith(quote) || s.length() == quote.length())) {
		            buf = new StringBuffer(s);
		            building = true;
		        } else {
		        	//if (min == 4) System.out.println("Split 2 >> " + i + " >>" + s + "<< ");
		           //System.out.println("]] >" + s + "<" + (s.startsWith(quote))
		           //         + " " + (! s.endsWith(quote)) + " " + (s.length() == 1));
		            temp[i++] = s;
		            keep = false;
		        }
		    }
		    if (building) {
//		        if (quote.equals(buf.substring(buf.length() - 1))) {
//		            buf.delete(buf.length() - 1, buf.length());
//		        }
		        temp[i++] = buf.toString();
		    }
		}

		ret = temp;
		newLength = Math.max(i, min + 1);
		if (newLength != temp.length) {
		    ret = new String[newLength];
		    for (j = 0; j < i; j++) {
		        ret[j] = temp[j];
		    }
		    for (j = i; j < newLength; j++) {
		        ret[j] = "";
		    }
		}

		return ret;
	}

	/**
	 * is quote present
	 * @param quote quote string
	 * @return if it defines a quote
	 */
	private boolean isQuote(String quote) {
	    return quote != null && quote.length() > 0;
	}

    /**
     * Return a basic CSV line parser
     * @return Returns the instance.
     */
    public static BasicCsvLineParser getInstance() {
        return instance;
    }
}
