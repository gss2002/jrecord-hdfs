/**
 * @Author Jean-Francois Gagnon
 * Created on 6/26/2006
 *
 * Purpose:
 *    Add support for Sign Separate Display Type numeric
 *
 * # Version 0.60 Bruce Martin 2007/02/16
 *   - Starting to seperate the Record package out from the RecordEditor
 *     so that it can be used seperately. So classes have been moved
 *     to the record package (ie RecordException + new Constant interface

 */
package net.sf.JRecord.Types;

import net.sf.JRecord.Common.IFieldDetail;
import net.sf.JRecord.Common.RecordException;

/**
 * Sign Seperate numeric (ie editted numeric in Cobol)
 *
 * @author Jean-Francois Gagnon
 *
 */
public class TypeSignSeparate extends TypeNum {

    private boolean isLeadingSign = true;
    /**
     * Define mainframe Zoned Decimal Type
     *
     * <p>This class is the interface between the raw data in the file
     * and what is to be displayed on the screen for Sign Separate Display numeric
     * fields.
     *
     * @param typeId Type Identifier
     */
    public TypeSignSeparate(final int typeId) {
        super(false, true, true, false, false);

        isLeadingSign = (typeId == Type.ftSignSeparateLead);

    }


    /**
     * @see net.sf.JRecord.Types.Type#getField(byte[], int, net.sf.JRecord.Common.FieldDetail)
     */
    public Object getField(byte[] record,
            final int position,
			final IFieldDetail field) {
        return addDecimalPoint(
                	fromSignSeparate(super.getFieldText(record, position, field)),
                	field.getDecimal());
    }


    /**
     * @see net.sf.JRecord.Types.Type#setField(byte[], int, net.sf.JRecord.Common.FieldDetail, java.lang.Object)
     */
    public byte[] setField(byte[] record,
            final int position,
			final IFieldDetail field,
			Object value)
    throws RecordException {

        String val = checkValue(field, toNumberString(value));
        copyRightJust(record, toSignSeparate(val, field),
	            position - 1, field.getLen(),
	            "0", field.getFontName());
	    return record;
    }

	@Override
	public String formatValueForRecord(IFieldDetail field, String value)
			throws RecordException {
		return toSignSeparate(checkValue(field, toNumberString(value)), field);
	}


	/**
	 * Convert a num to a Sign Separate String
	 *
	 * @param num  Numeric string
	 * @param field  Field Detail
	 *
	 * @return number-string
	 * @throws RecordException any errors generated in the
	 * conversion
	 */
	private String toSignSeparate(String num,
                                  IFieldDetail field)
    throws RecordException {


		if (num == null || num.length() == 0 || num.equals("-") || num.equals("+")) {
			// throw ...
			return paddingString("+", field.getLen(), '0', !isLeadingSign);
		}
		String ret = num.trim();
        String sign = "";

		if (num.startsWith("-")) {
            sign = "-";
            ret = ret.substring(1);
        } else {
            sign = "+";
            if (num.startsWith("+")) {
                ret = ret.substring(1);
            }

		}

        if (ret.length() >= field.getLen() && field.isFixedFormat()) {
            throw new RecordException("Value: " + ret + " is too large to fit field");
        }

        ret = paddingString(ret, field.getLen() - 1, '0', true);

        if (isLeadingSign) {
            ret = sign + ret;
        } else {
            ret = ret + sign;
        }

		return ret;
	}

    /**
     * Convert a Sign Separate Number String to a number string
     *
     * @param numSignSeparate Zoned Numeric string
     *
     * @return number-string
     */
    private String fromSignSeparate(String numSignSeparate) {
         if (numSignSeparate == null || numSignSeparate.length() == 0 || numSignSeparate.equals("-")) {
            // throw ...
            return "";
        }

         String ret;
         String sign = "";


        ret = numSignSeparate.trim();
        if (isLeadingSign) {
            if (ret.length() > 0 && ret.charAt(0) == '+') {
            	ret = ret.substring(1);    
            }
        } else {
			int lastIdx = ret.length() - 1;
			if (ret.length() > 0 && (ret.charAt(lastIdx) == '+' || ret.charAt(lastIdx) == '-')) {
			    sign = ret.substring(lastIdx);
			    ret = ret.substring(0, lastIdx);
			}
        }

        if ("-".equals(sign)) {
            ret = sign + ret;
        }

        return ret;

    }

  /**
   * Pad a string S with a size of N with char C
   * on the left (True) or on the right(false)
   *
   * @param s String to be padded
   * @param n Desired Length
   * @param c Padding character
   * @param paddingLeft true for Left, false for Right
   *
   * @return padded String
   **/
  private String paddingString(String s, int n, char c, boolean paddingLeft) {
    StringBuffer str = new StringBuffer(s);
    int strLength  = str.length();
    if (n > 0 && n > strLength) {
      for (int i = 0; i <= n; i++) {
            if (paddingLeft) {
              if (i < n - strLength) {
                  str.insert(0, c);
              }
            } else {
              if (i > strLength) {
                  str.append(c);
              }
            }
      }
    }
    return str.toString();
  }

}

