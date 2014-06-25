/*************************************************************
 * This file is part of CB2XML.
 * See the file "LICENSE" for copyright information and the
 * terms and conditions for copying, distribution and
 * modification of CB2XML.
 *************************************************************
 */

package net.sf.cb2xml;

import java.io.File;
import java.io.InputStream;
import java.io.PushbackReader;
import java.io.StringReader;

import net.sf.cb2xml.sablecc.lexer.Lexer;
import net.sf.cb2xml.sablecc.node.Start;
import net.sf.cb2xml.sablecc.parser.Parser;
import net.sf.cb2xml.sablecc.parser.ParserException;
import net.sf.cb2xml.util.XmlUtils;

import org.w3c.dom.Document;

/**
 * main utility for parsing a copybook into XML
 * calls pre-processor, then parser to perform parse
 * note the debug mode to view detailed SableCC debug output
 *
 * @author Peter Thomas
 */

public class Cb2Xml {

	public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage:\tcb2xml <copybookFileName> [debug]");
            return;
        }

		File file = new File(args[0]);
		boolean debug = false;
		if (args.length > 1) {
			debug = true;
		}
		String result = convertToXMLString(file, debug);
        System.out.println(result);
	}

	// public API methods
	public static Document convertToXMLDOM(File file) {
		return convert(file, null, null, false);
	}

	public static String convertToXMLString(File file) {
		Document document = convert(file, false);
		return XmlUtils.domToString(document).toString();
	}

	// overloaded methods for debug mode
	public static Document convertToXMLDOM(File file, boolean debug) {
		return convert(file, debug);
	}

	public static String convertToXMLString(File file, boolean debug) {
		Document document = convert(file, debug);
		return XmlUtils.domToString(document).toString();
	}

	public static Document convertToXMLDOM(InputStream is, String name) {
		return convert(null, is, name, false);
	}

	public static String convertToXMLString(InputStream is, String name) {
		Document document = convert(null, is, name, false);
		return XmlUtils.domToString(document).toString();
	}

	// overloaded methods for debug mode
	public static Document convertToXMLDOM(InputStream is, String name, boolean debug) {
		return convert(null, is, name, debug);
	}

	public static String convertToXMLString(InputStream is, String name, boolean debug) {
		Document document = convert(null, is, name, debug);
		return XmlUtils.domToString(document).toString();
	}


	private static Document convert(File file, boolean debug) {
		return convert(file, null, null, debug);
	}


	private static Document convert(File file, InputStream is, String copybookName, boolean debug) {
		Document document = null;
		Lexer lexer = null;
		String preProcessed = null;
		String name = copybookName;
		try {
			if (is == null) {
				preProcessed = CobolPreprocessor.preProcess(file);
				name = file.getName();
			} else {
				preProcessed = CobolPreprocessor.preProcess(is);
			}
			StringReader sr = new StringReader(preProcessed);
			PushbackReader pbr = new PushbackReader(sr, 1000);
			if (debug) {
				System.err.println("*** debug mode ***");
				lexer = new DebugLexer(pbr);
			} else {
				lexer = new Lexer(pbr);
			}
			Parser parser = new Parser(lexer);
			Start ast = parser.parse();
			CopyBookAnalyzer copyBookAnalyzer = new CopyBookAnalyzer(name, parser);
			ast.apply(copyBookAnalyzer);
			document = copyBookAnalyzer.getDocument();
		} catch(ParserException pe) {
			System.err.println("*** fatal parse error ***");
			System.err.println(pe.getMessage());
			if (debug) {
				System.err.println("=== buffer dump start ===");
				System.err.println(((DebugLexer) lexer).getBuffer());
				System.err.println("=== buffer dump end ===");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return document;
	}
}