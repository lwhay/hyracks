package edu.uci.ics.hyracks.hdmlc.driver;

import java.io.FileReader;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import edu.uci.ics.hyracks.hdmlc.ast.CompilationUnit;
import edu.uci.ics.hyracks.hdmlc.codegen.HDMLCodeGenerator;
import edu.uci.ics.hyracks.hdmlc.compiler.HDMLCompiler;
import edu.uci.ics.hyracks.hdmlc.typesystem.TypeSystem;

public class HDMLC {
    public static void main(String[] args) throws Exception {
        CmdLineOptions clo = new CmdLineOptions();
        CmdLineParser clp = new CmdLineParser(clo);
        try {
            clp.parseArgument(args);
        } catch (CmdLineException e) {
            clp.printUsage(System.err);
            throw e;
        }

        HDMLCompiler hdmlc = new HDMLCompiler();
        CompilationUnit cu = hdmlc.parse(new FileReader(clo.args.get(0)));
        if (clo.printAST) {
            XStream xs = new XStream(new DomDriver());
            xs.toXML(cu, System.err);
        }
        TypeSystem ts = hdmlc.compile(cu);
        HDMLCodeGenerator codegen = new HDMLCodeGenerator(ts);
        codegen.codegen();
    }
}