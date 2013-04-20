package com.swijaya.hostess;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HostessTest {

    private static final LongWritable ZERO = new LongWritable();
    private static final IntWritable ONE = new IntWritable(1);

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        Hostess.Map mapper = new Hostess.Map();
        Hostess.Reduce reducer = new Hostess.Reduce();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper_EmptyLine() {
        mapDriver.withInput(ZERO, new Text(""))
                .runTest();
        mapDriver.withInput(ZERO, new Text("     \t      "))
                .runTest();
        mapDriver.withInput(ZERO, new Text("\t"))
                .runTest();
        mapDriver.withInput(ZERO, new Text(" "))
                .runTest();
    }

    @Test
    public void testMapper_CommentLine() {
        mapDriver.withInput(ZERO, new Text("#"))
                .runTest();
        mapDriver.withInput(ZERO, new Text("\t#"))
                .runTest();
        mapDriver.withInput(ZERO, new Text(" #"))
                .runTest();
        mapDriver.withInput(ZERO, new Text(" #this is a comment line"))
                .runTest();
        mapDriver.withInput(ZERO, new Text(" # this is a comment line"))
                .runTest();
    }

    @Test
    public void testMapper_InvalidAddress() {
        mapDriver.withInput(ZERO, new Text("320.1.1.1"))
                .runTest();
        mapDriver.withInput(ZERO, new Text("\t320.1.1.1\thostname"))
                .runTest();
        mapDriver.withInput(ZERO, new Text("\tfe80::1%lo0"))
                .runTest();
        mapDriver.withInput(ZERO, new Text("\tge80::1\thostname"))
                .runTest();
    }

    @Test
    public void testMapper_OnlyAddress() {
        // a line with only an address
        mapDriver.withInput(ZERO, new Text("120.1.1.1"))
                .runTest();
        mapDriver.withInput(ZERO, new Text("\t120.1.1.1\t"))
                .runTest();
        mapDriver.withInput(ZERO, new Text("\tfe80::1"))
                .runTest();
        mapDriver.withInput(ZERO, new Text("     fe80::1    \t"))
                .runTest();

        // a line with only an address and inline comment
        mapDriver.withInput(ZERO, new Text("120.1.1.1 #this is a comment"))
                .runTest();
        mapDriver.withInput(ZERO, new Text("\t120.1.1.1\t# this is a comment"))
                .runTest();
        mapDriver.withInput(ZERO, new Text("\tfe80::1 #this is a comment"))
                .runTest();
        mapDriver.withInput(ZERO, new Text("     fe80::1    \t   # this is a comment"))
                .runTest();
    }

    @Test
    public void testMapper_ValidLine_1() {
        mapDriver.withInput(ZERO, new Text("127.0.0.1\tlocalhost"))
                .withOutput(new Text("127.0.0.1->localhost"), ONE)
                .runTest();
    }

    @Test
    public void testMapper_ValidLine_2() {
        mapDriver.withInput(ZERO, new Text("\t::1\tlocalhost\thostname"))
                .withOutput(new Text("::1->localhost"), ONE)
                .withOutput(new Text("::1->hostname"), ONE)
                .runTest();
    }

    @Test
    public void testMapper_ValidLine_3() {
        mapDriver.withInput(ZERO, new Text("128.0.0.1\tlocalhost    # this is a comment"))
                .withOutput(new Text("128.0.0.1->localhost"), ONE)
                .runTest();
    }

    @Test
    public void testMapper_ValidLine_4() {
        mapDriver.withInput(ZERO, new Text("\t::1\tlocalhost\thostname\t#this is a comment"))
                .withOutput(new Text("::1->localhost"), ONE)
                .withOutput(new Text("::1->hostname"), ONE)
                .runTest();
    }

    @Test
    public void testReducer_SingleRecord() {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(ONE);
        reduceDriver.withInput(new Text("::1->localhost"), values)
                .withOutput(new Text("::1"), new Text("localhost"))
                .runTest();
    }

    @Test
    public void testReducer_GroupedRecords() {
        // this indicates duplicates in the original input set
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(ONE);
        values.add(ONE);
        // expect only one final record in the output
        reduceDriver.withInput(new Text("127.0.0.1->localhost"), values)
                .withOutput(new Text("127.0.0.1"), new Text("localhost"))
                .runTest();
    }

}
