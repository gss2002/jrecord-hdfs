/*
 * Created on 8/05/2004
 *
 *  This class represents a group of records
 *
 * Modification log:
 * On 2006/06/28 by Jean-Francois Gagnon:
 *    - Made sure a Group of Records is tested to see if
 *      there is binary format to be handled
 *
 * Version 0.61 (2007/03/29)
 *    - CSV Split for when there a blank columns in the CSV file
 */
package net.sf.JRecord.Details;

import java.util.HashMap;
import java.util.Map;

import net.sf.JRecord.Common.CommonBits;
import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.Conversion;
import net.sf.JRecord.Common.FieldDetail;
import net.sf.JRecord.Common.IBasicFileSchema;
import net.sf.JRecord.Common.IFieldDetail;
import net.sf.JRecord.Common.RecordException;
import net.sf.JRecord.CsvParser.ICsvLineParser;
import net.sf.JRecord.CsvParser.BinaryCsvParser;
import net.sf.JRecord.CsvParser.CsvDefinition;
import net.sf.JRecord.CsvParser.ParserManager;
import net.sf.JRecord.Types.Type;
import net.sf.JRecord.Types.TypeManager;




/**
 * This class represents a <b>record-layout</b> description. i.e.
 * It describes the Structure of both a File and the lines in it.
 * <p>A <b>Layout</b> can have
 * one or more <b>Records</b> (class RecordDetail) which intern
 * holds one or more <b>fields</b> (class FieldDetail).
 *
 * <pre>
 *     LayoutDetail  - Describes a file
 *       |
 *       +----- RecordDetail (1 or More) - Describes one record in the file
 *                |
 *                +------  FieldDetail (1 or More)  - Describes one field in the file
 * </pre>
 *
 * <p>There are several ways to load a RecordLayout
 * <pre>
 * <b>Loading an RecordEditor-XML:</b>
 *          LayoutDetail layout = CopybookLoaderFactory.getInstance().getLayoutRecordEditXml(copybookName, null);
 *
 * <b>Using the Loader Factory CopybookLoaderFactory:</b>
 *          CopybookLoader loader = CopybookLoaderFactory.getInstance()
 *                  .getLoader(CopybookLoaderFactory.RECORD_EDITOR_XML_LOADER);
 *          LayoutDetail layout = loader.loadCopyBook(copybookName, 0, 0, "", 0, 0, null).asLayoutDetail();
 *
 * <b>Creating the loader:</b>
 *          CopybookLoader loader = new RecordEditorXmlLoader();
 *          LayoutDetail layout = loader.loadCopyBook(copybookName, 0, 0, "", 0, 0, null).asLayoutDetail();
 *
 * @param pLayoutName Record Layout Name
 * @param pRecords All the sub records
 * @param pDescription Records Description
 * @param pLayoutType Record Type
 * @param pRecordSep Record Separator
 * @param pEolIndicator End of line indicator
 * @param pFontName Canonical Name
 * @param pRecordDecider used to decide which layout to use
 * @param pFileStructure file structure
 *
 *
 * @author Bruce Martin
 * @version 0.55
 *
 */
public class LayoutDetail implements IBasicFileSchema {

	private String layoutName;
	private String description;
	private byte[] recordSep;
	private int layoutType;
	private RecordDetail[] records;
	private boolean binary = false;
	private String fontName = "";
	private String eolString;
	//private TypeManager typeManager;
	private RecordDecider decider;

	private HashMap<String, IFieldDetail> fieldNameMap = null;
	private HashMap<String, IFieldDetail> recordFieldNameMap = null;
	private String delimiter = "";
	private int fileStructure;

	private int recordCount;

	private boolean treeStructure = false;

	private final boolean multiByteCharset, csvLayout;

	/**
	 * This class holds a one or more records
	 *
	 * @param pLayoutName Record Layout Name
	 * @param pRecords All the sub records
	 * @param pDescription Records Description
	 * @param pLayoutType Record Type
	 * @param pRecordSep Record Separator
	 * @param pEolIndicator End of line indicator
	 * @param pFontName Canonical Name
	 * @param pRecordDecider used to decide which layout to use
	 * @param pFileStructure file structure
	 *
	 * @throws RecordException multiple field delimiters used
	 */
	public LayoutDetail(final String pLayoutName,
	        		   final RecordDetail[] pRecords,
	        		   final String pDescription,
	        		   final int pLayoutType,
	        		   final byte[] pRecordSep,
	        		   final String pEolIndicator,
	        		   final String pFontName,
	        		   final RecordDecider pRecordDecider,
	        		   final int pFileStructure)
	{
	    super();

        int i, j;
        boolean first = true;
        //int lastSize = -1;


	    this.layoutName    = pLayoutName;
		this.records       = pRecords;
		this.description   = pDescription;
		this.layoutType    = pLayoutType;
		this.recordSep     = pRecordSep;
		this.fontName      = pFontName;
		this.decider       = pRecordDecider;
		this.fileStructure = pFileStructure;
		this.recordCount   = pRecords.length;

		if (fontName == null) {
		    fontName = "";
		}
		this.multiByteCharset = Conversion.isMultiByte(fontName);

		while (recordCount > 0 && pRecords[recordCount - 1] == null) {
		    recordCount -= 1;
		}

		if (recordSep == null) {
			if (fontName == null || "".equals(fontName)) {
				recordSep = Constants.SYSTEM_EOL_BYTES;
			} else {
				recordSep = CommonBits.getEolBytes(null, "", fontName);
			}
			
			recordSep = CommonBits.getEolBytes(recordSep, pEolIndicator, fontName);
		}

		if (Constants.DEFAULT_STRING.equals(pEolIndicator)
		||  pRecordSep == null) {
		    eolString = System.getProperty("line.separator");
		    if (recordSep != null && recordSep.length < eolString.length()) {
		    	eolString = Conversion.toString(recordSep, pFontName);
		    }
		} else {
		    eolString = Conversion.toString(pRecordSep, pFontName);
		}


		switch (pLayoutType) {
			case Constants.rtGroupOfBinaryRecords:
			case Constants.rtFixedLengthRecords:
			case Constants.rtBinaryRecord:
			    binary = true;
			break;
            case Constants.rtGroupOfRecords:
			case Constants.rtRecordLayout:
			    if (recordCount >= 1) {
			        int numFields;
			        for (j = 0; (! binary) && j < recordCount; j++) {
			            numFields =  pRecords[j].getFieldCount();
			            for (i = 0; (! binary) && i < numFields; i++) {
			                binary = pRecords[j].isBinary(i);
			            }
			        }
			    }
			break;
			default:
		}

		boolean csv = false;
	    for (j = 0; j < recordCount; j++) {
	    	RecordDetail record =  pRecords[j];
	    	if ((record.getRecordType() == Constants.rtDelimitedAndQuote
			          || record.getRecordType() == Constants.rtDelimited)) {
	    		csv = true;
	    	}
	    	if (record != null && record.getFieldCount() > 0) {
//	    		if ((lastSize >= 0 && lastSize != record.getLength())
//	    		||  (record.getField(record.getFieldCount() - 1).getType()
//	    				== Type.ftCharRestOfRecord )){
//	    			fixedLength = false;
//	    		}
	    		//lastSize = record.getLength();

		    	treeStructure = treeStructure || (record.getParentRecordIndex() >= 0);
		        if ((record.getRecordType() == Constants.rtDelimitedAndQuote
		          || record.getRecordType() == Constants.rtDelimited)
		        &&  (!delimiter.equals(record.getDelimiter()))) {
//		        	fixedLength = false;
		            if (first) {
		                delimiter = record.getDelimiter();
		                first = false;
		            } else if (! delimiter.equals(record.getDelimiter())) {
		                throw new RuntimeException(
		                        	"only one field delimiter may be used in a Detail-Group "
		                        +   "you have used \'" + delimiter
		                        +   "\' and \'"
		                        +  record.getDelimiter() + "\'"
		                );
		            }
		        }
	    	}
	    }
	    csvLayout = csv;
	}


	/**
	 * get a specified field
	 *
	 * @param layoutIdx the specific record layout to be used
	 * @param fieldIdx field index required
	 * @return the required field
	 */
	public FieldDetail getField(final int layoutIdx, final int fieldIdx) {
		return records[layoutIdx].getField(fieldIdx);
	}

	/**
	 * Get the field Description array
	 *
	 * @param layoutIdx layout that we want the description for
	 * @return all the descriptions
	 */
	public String[] getFieldDescriptions(final int layoutIdx, int columnsToSkip) {
	    if (layoutIdx >= recordCount) {
	        return null;
	    }
	    RecordDetail rec = records[layoutIdx];
		String[] ret = new String[rec.getFieldCount() - columnsToSkip];
		int i, idx;

		for (i = 0; i < rec.getFieldCount() - columnsToSkip; i++) {
		    idx = getAdjFieldNumber(layoutIdx, i + columnsToSkip);
			ret[i] = rec.getField(idx).getDescription();
			if (ret[i] == null || "".equals(ret[i])) {
			    ret[i] = rec.getField(idx).getName();
			}
		}

		return ret;
	}


	/**
	 * Get the records Description.
	 *
	 * @return The description
	 */
	public String getDescription() {
		return description;
	}


	/**
	 * Get the record-layout name
	 *
	 * @return record layout name
	 */
	public String getLayoutName() {
		return layoutName;
	}

	/**
	 * Get all the record Details.
	 *
	 * @return all the record layouts
	 * @deprecated use getRecord instead
	 */
	public RecordDetail[] getRecords() {
		return records;
	}

	/**
	 * Add a record to the layout
	 * @param record new record
	 */
	public void addRecord(RecordDetail record) {
	    if (recordCount >= records.length) {
	    	RecordDetail[] temp = records;
	        records = new RecordDetail[recordCount + 5];
	        System.arraycopy(temp, 0, records, 0, temp.length);
	        recordCount = temp.length;
	    }
	    records[recordCount] = record;
	    recordCount += 1;
	}


	/**
	 * get a specific field number
	 *
	 * @param recordNum record number to retrieve
	 *
	 * @return a specific record layout
	 */
	public RecordDetail getRecord(int recordNum) {
	    if (recordNum < 0 || records.length == 0) {
	        return null;
	    }
		return records[recordNum];
	}


	/**
	 * get number of records in the layout
	 *
	 *
	 * @return the number of records in the layout
	 */
	public int getRecordCount() {
		return recordCount;
	}


	/**
	 * get record type
	 *
	 * @return the Record Type
	 */
	public int getLayoutType() {
		return layoutType;
	}



	/**
	 * Get the record Seperator bytes
	 *
	 * @return Record Seperator
	 */
	public byte[] getRecordSep() {
		return recordSep;
	}


	/**
	 * wether it is a binary record
	 *
	 * @return wether it is a binary record
	 */
    public boolean isBinary() {
        return binary;
    }


    /**
     * Get the Charset Name (ie Font name)
     *
     * @return Charset Name (ie Font name)
     */
    public String getFontName() {
        return fontName;
    }

    

    @Override
	public String getQuote() {
		return records[0].getQuote();
	}


	/**
     * Get the seperator String
     *
     * @return end of line string
     */
    public String getEolString() {
        return eolString;
    }


    /**
     * Get the maximum length of the Layout
     *
     * @return the maximum length
     */
    public int getMaximumRecordLength() {
        int i;
        int maxSize = 0;
		for (i = 0; i < recordCount; i++) {
			maxSize = java.lang.Math.max(maxSize, records[i].getLength());
		}

		return maxSize;
    }


    /**
     * Return the file structure
     *
     * @return file structure
     */
    public int getFileStructure() {
        int ret = fileStructure;

        if (fileStructure == Constants.IO_NAME_1ST_LINE &&  isBinCSV()) {
        	ret = Constants.IO_BIN_NAME_1ST_LINE;
        } else if (fileStructure > Constants.IO_TEXT_LINE) {
        } else if (fileStructure == Constants.IO_TEXT_LINE) {
			ret = checkTextType();
        } else if (getLayoutType() == Constants.rtGroupOfBinaryRecords
               &&  recordCount > 1) {
		    ret = Constants.IO_BINARY;
		} else if (isBinary()) {
		    ret = Constants.IO_FIXED_LENGTH;
		} else if ( isBinCSV()) {
			ret = Constants.IO_BIN_TEXT;
		} else if (fontName != null && ! "".equals(fontName)){
		    ret = Constants.IO_TEXT_LINE;
		} else {
			ret = checkTextType();
		}
       //System.out.println(" ~~ getFileStructure " + fileStructure + " " + ret);

		return ret;
    }

    private int checkTextType() {
    	int ret = fileStructure;
    	if ( isBinCSV()) {
			ret = Constants.IO_BIN_TEXT;
		} else if (multiByteCharset) {
    		return Constants.IO_UNICODE_TEXT;
		} else if (fontName != null && ! "".equals(fontName)){
		    ret = Constants.IO_TEXT_LINE;
		} else {
			ret = Constants.IO_BIN_TEXT;
		}

    	return ret;
	}
    /**
     * Get the Index of a specific record (base on name)
     *
     * @param recordName record name being searched for
     *
     * @return index of the record
     */
    public int getRecordIndex(String recordName) {
        int ret = Constants.NULL_INTEGER;
        int i;

        if (recordName != null) {
            for (i = 0; i < recordCount; i++) {
                if (recordName.equalsIgnoreCase(records[i].getRecordName())) {
                    ret = i;
                    break;
                }
            }
        }
        return ret;
    }


    /**
     * Get the Record Decider class (if present)
     * @return Returns the record layout decider.
     */
    public RecordDecider getDecider() {
        return decider;
    }


    /**
     * Get a fields value
     *
     * @param record record containg the field
     * @param type type to use when getting the field
     * @param field field to retrieve
     *
     * @return fields Value
     */
    public Object getField(final byte[] record, int type, IFieldDetail field) {

    	//System.out.print(" ---> getField ~ 1");
        if (field.isFixedFormat()) {
            return TypeManager.getSystemTypeManager().getType(type) //field.getType())
					.getField(record, field.getPos(), field);
        }

        if (isBinCSV()) {
        	//System.out.print(" 3 ");
        	String value = (new BinaryCsvParser(delimiter)).getValue(record, field);

        	return formatField(field,  type, value);
        } else {
	        return formatCsvField(field,  type, Conversion.toString(record, field.getFontName()));
        }
    }

    public final Object formatCsvField(IFieldDetail field,  int type, String value) {
        ICsvLineParser parser = ParserManager.getInstance().get(field.getRecord().getRecordStyle());
        String val = parser.getField(field.getPos() - 1,
        		value,
        		new CsvDefinition(delimiter, field.getQuote()));

        return formatField(field,  type, val);
    }


    private Object formatField(IFieldDetail field,  int type, String value) {

        //System.out.print(" ~ " + delimiter + " ~ " + new String(record));

        if (value != null && ! "".equals(value)) {
        	byte[] rec = Conversion.getBytes(value, field.getFontName());
            FieldDetail fldDef
        		= new FieldDetail(field.getName(), "", type,
        		        		   field.getDecimal(), field.getFontName(),
        		        		   field.getFormat(), field.getParamater());

            fldDef.setRecord(field.getRecord());

            fldDef.setPosLen(1, rec.length);

//            System.out.println(" ~ " + TypeManager.getSystemTypeManager().getType(type)
//					.getField(Conversion.getBytes(value, font),
//					          1,
//					          fldDef));
            return TypeManager.getSystemTypeManager().getType(type)
					.getField(rec,
					          1,
					          fldDef);
        }
        //System.out.println();

        return "";
    }


    /**
     * Set a fields value
     *
     * @param record record containg the field
     * @param field field to retrieve
     * @param value value to set
     *
     * @return byte[] updated record
     *
     * @throws RecordException any conversion error
     */
    public byte[] setField(byte[] record, IFieldDetail field, Object value)
    throws RecordException {
        return setField(record, field.getType(), field, value);
    }

    /**
     * Set a fields value
     *
     * @param record record containg the field
     * @param type type to use in the conversion
     * @param field field to retrieve
     * @param value value to set
     *
     * @return byte[] updated record
     *
     * @throws RecordException any conversion error
     */
    public byte[] setField(byte[] record, int type, IFieldDetail field, Object value)
    throws RecordException {
        if (field.isFixedFormat()) {
            record = TypeManager.getSystemTypeManager().getType(type)
				.setField(record, field.getPos(), field, value);
        } else  {
            String font = field.getFontName();
            ICsvLineParser parser = ParserManager.getInstance().get(field.getRecord().getRecordStyle());

            Type typeVal = TypeManager.getSystemTypeManager().getType(type);
            String s = typeVal.formatValueForRecord(field, value.toString());
            //System.out.println(" ---> setField ~ " + delimiter + " ~ " + s + " ~ " + new String(record));
            if  (isBinCSV()) {
             	record = (new BinaryCsvParser(delimiter)).updateValue(record, field, s);
            } else {
	            String newLine = parser.setField(field.getPos() - 1,
	            		typeVal.getFieldType(),
	            		Conversion.toString(record, font),
	            		new CsvDefinition(delimiter, field.getQuote()), s);

                record = Conversion.getBytes(newLine, font);
            }
        }
        //System.out.println(" ---> setField ~ Done");
        return record;
    }


    /**
     * Get a field for a supplied field-name
     *
     * @param fieldName name of the field being requested
     *
     * @return field definition for the supplied name
     */
    public IFieldDetail getFieldFromName(String fieldName) {
    	IFieldDetail ret = null;
    	String key = fieldName.toUpperCase();

    	buildFieldNameMap();

    	if (fieldNameMap.containsKey(key)) {
    		ret = fieldNameMap.get(key);
    	}

    	return ret;
    }

    private void buildFieldNameMap() {

    	if (fieldNameMap == null) {
    		int i, j, k, size;
    		IFieldDetail fld;
    		String name, nameTmp;

    		size = 0;
    		for (i = 0; i < recordCount; i++) {
    		    size += records[i].getFieldCount();
    		}
    		size = (size * 5) / 4 + 4;

    		fieldNameMap = new HashMap<String, IFieldDetail>(size);
    		recordFieldNameMap = new HashMap<String, IFieldDetail>(size);

    		for (i = 0; i < recordCount; i++) {
    			//FieldDetail[] flds = records[i].getFields();
    			for (j = 0; j < records[i].getFieldCount(); j++) {
    			    fld = records[i].getField(j);
    			    nameTmp = fld.getName();
    			    name = nameTmp;
    			    nameTmp = nameTmp + "~";
    			    k = 1;
    			    while (fieldNameMap.containsKey(name.toUpperCase())) {
    			    	name = nameTmp + k++;
    			    }
    			    fld.setLookupName(name);
					fieldNameMap.put(name.toUpperCase(), fld);
    				recordFieldNameMap.put(
    						records[i].getRecordName() + "." + name.toUpperCase(),
    						fld);
    			}
    			records[i].setNumberOfFieldsAdded(0);
    		}
     	} else if (this.isBuildLayout()) {
     		int j;
     		IFieldDetail fld;

       		for (int i = 0; i < recordCount; i++) {
       			if (records[i].getNumberOfFieldsAdded() > 0) {
       				for (j = 1; j <=  records[i].getFieldCount(); j++) {
       	   			    fld = records[i].getField(records[i].getFieldCount() - j);
//       	   			    System.out.println("Adding ... " + (records[i].getFieldCount() - j)
//       	   			    		+ " " + fld.getName());
        				fieldNameMap.put(fld.getName().toUpperCase(), fld);
        				recordFieldNameMap.put(
        						records[i].getRecordName() + "." + fld.getName().toUpperCase(),
        						fld);

      				}
       	    		records[i].setNumberOfFieldsAdded(0);
      			}
       		}

    	}
    }

	/**
	 * get the field delimiter
	 * @return the field delimeter
	 */
    public String getDelimiter() {
        return delimiter;
    }




	/**
	 * get the field delimiter
	 * @return the field delimeter
	 */
    public byte[] getDelimiterBytes() {
    	byte[] ret;
    	if (isBinCSV()) {
    		ret = new byte[1];
    		ret[0] = Conversion.getByteFromHexString(delimiter);
    	} else {
    		ret = Conversion.getBytes(delimiter, fontName);
    	}
        return ret;
    }


    /**
     * set the delimiter
     * @param delimiter new delimiter
     */
    public void setDelimiter(String delimiter) {
    	String delim = RecordDetail.convertFieldDelim(delimiter);
    	if (this.records != null) {
    		for (int i=0; i < records.length; i++) {
    			records[i].setDelimiter(delim);
    		}
    	}
		this.delimiter = delim;
	}


	/**
     /**
     * This is a function used by the RecordEditor to get a field using
     * recalculated columns. Basically for XML copybooks,
     * the End and FollowingText columns
     * are moved from from 2 and 3 to  the end of the record.
     * Function probably does not belong here but as good as any spot.
     *
     * @param layoutIdx  record index
     * @param fieldIdx field index
     * @return requested field
     */
    public FieldDetail getAdjField(int layoutIdx, int fieldIdx) {
        return getRecord(layoutIdx)
                       .getField(getAdjFieldNumber(layoutIdx, fieldIdx));
    }


    /**
     * This is a function used by the RecordEditor to recalculate
     * columns. Basically for XML copybooks, the End and FollowingText
     * columns are moved from from 2 and 3 to  the end of the record.
     * Function probably does not belong here but as good as any spot.
     *
     * @param recordIndex record index
     * @param inColumn input column
     * @return adjusted column
     */
    public int getAdjFieldNumber(int recordIndex, int inColumn) {

	    int ret = inColumn;

	    //System.out.println("~~ " + inColumn + " " + ret + " "
	    //        + layout.getRecord(layoutIndex).getRecordName() + " " + layout.getRecord(layoutIndex).getFieldCount());

	    if (ret > 0 && getFileStructure() == Constants.IO_XML_BUILD_LAYOUT) {
	        int len = getRecord(recordIndex).getFieldCount();

            if (ret > len - 3) {
                ret -= len - 3;
            } else {
                ret += 2;
            }
	    }
	    return ret;
    }

    public int getUnAdjFieldNumber(int recordIndex, int inColumn) {

	    int ret = inColumn;

	    if (ret > 0 && getFileStructure() == Constants.IO_XML_BUILD_LAYOUT) {
	        int len = getRecord(recordIndex).getFieldCount();

            if (ret < 3) {
                ret += len - 3;
            } else {
                ret -= 2;
            }
	    }
	    return ret;
    }

    /**
     * Get a Map of all FieldNames - Field Definitions
     * @return Map of all FieldNames - Field Definitions
     */
    public Map<String, IFieldDetail> getFieldNameMap() {
    	buildFieldNameMap();
		return new HashMap<String, IFieldDetail>(fieldNameMap);
	}

    /**
     * Get a map keyed on RecordName.FieldName ~ Field Definition
     * @return Get a map keyed on RecordName.FieldName ~ Field Definition
     */
	public Map<String, IFieldDetail> getRecordFieldNameMap() {
		buildFieldNameMap();
		return new HashMap<String, IFieldDetail>(recordFieldNameMap);
	}


	public final boolean isCsvLayout() {
		return csvLayout;
	}


	/**
     * Wether this is an XML Layout
     * @return is it an XML layout
     */
    public final boolean isXml() {
        return fileStructure == Constants.IO_XML_USE_LAYOUT
            || fileStructure == Constants.IO_XML_BUILD_LAYOUT;
    }

    /**
     * Wether it is ok to add Attributes to this layout
     * @return Wether it is ok to add Attributes to this layout
     */
    public final boolean isOkToAddAttributes() {
    	return fileStructure == Constants.IO_XML_BUILD_LAYOUT;
    }

    /**
     * determine wether the layout is built or not
     * @return determine wether the layout is built or not
     */
    public final boolean isBuildLayout() {
    	return fileStructure == Constants.IO_XML_BUILD_LAYOUT
    	    || fileStructure == Constants.IO_NAME_1ST_LINE;
    }


	/**
	 * check if a Tree Structure has been defined for the layout
	 * i.e. is there a hierarchy between the various layouts
	 * @return wether there is a Tree Structure defined
	 */
	public final boolean hasTreeStructure() {
		return treeStructure;
	}

	public boolean isBinCSV() {
		return	   delimiter != null 
				&& delimiter.length() > 3
				&& delimiter.toLowerCase().startsWith("x'");
	}
}

