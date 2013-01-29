package edu.uci.ics.hyracks.hdmlc.driver;

import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

public class CmdLineOptions {
    @Option(name = "-print-ast", required = false, usage = "Prints the AST")
    public boolean printAST = false;

    @Argument(multiValued = true)
    public List<String> args;
}