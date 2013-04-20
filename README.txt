Input
=====

A collection of HOSTS file.

Output
======

A single, merged HOSTS file with duplicates removed and host aliases in a
single line flattened into multiple lines.

Unit Tests
==========

Using [MRUnit](http://mrunit.apache.org/). To execute, using Maven:

    $ mvn test
    [INFO] Scanning for projects...
    [INFO]                                                                         
    [INFO] ------------------------------------------------------------------------
    [INFO] Building Hostess 1.0-SNAPSHOT
    [INFO] ------------------------------------------------------------------------
    [INFO] 
    [INFO] --- maven-resources-plugin:2.3:resources (default-resources) @ Hostess ---
    [INFO] Using 'UTF-8' encoding to copy filtered resources.
    [INFO] Copying 1 resource
    [INFO] 
    [INFO] --- maven-compiler-plugin:3.1:compile (default-compile) @ Hostess ---
    [INFO] Nothing to compile - all classes are up to date
    [INFO] 
    [INFO] --- maven-resources-plugin:2.3:testResources (default-testResources) @ Hostess ---
    [INFO] Using 'UTF-8' encoding to copy filtered resources.
    [INFO] skip non existing resourceDirectory /home/santa/Code/projects/Hostess/src/test/resources
    [INFO] 
    [INFO] --- maven-compiler-plugin:3.1:testCompile (default-testCompile) @ Hostess ---
    [INFO] Nothing to compile - all classes are up to date
    [INFO] 
    [INFO] --- maven-surefire-plugin:2.10:test (default-test) @ Hostess ---
    [INFO] Surefire report directory: /home/santa/Code/projects/Hostess/target/surefire-reports

    -------------------------------------------------------
     T E S T S
     -------------------------------------------------------
     Running com.swijaya.hostess.HostessTest
     13/04/19 19:20:49 WARN hostess.Hostess$Map: (file pos: 0) Not a valid address: 320.1.1.1
     13/04/19 19:20:49 WARN hostess.Hostess$Map: (file pos: 0) Not a valid address: 320.1.1.1
     13/04/19 19:20:49 WARN hostess.Hostess$Map: (file pos: 0) Not a valid address: fe80::1%lo0
     13/04/19 19:20:49 WARN hostess.Hostess$Map: (file pos: 0) Not a valid address: ge80::1
     13/04/19 19:20:49 WARN hostess.Hostess$Map: (file pos: 0) Record only contains an address: 120.1.1.1
     13/04/19 19:20:49 WARN hostess.Hostess$Map: (file pos: 0) Record only contains an address: 120.1.1.1
     13/04/19 19:20:49 WARN hostess.Hostess$Map: (file pos: 0) Record only contains an address: fe80::1
     13/04/19 19:20:49 WARN hostess.Hostess$Map: (file pos: 0) Record only contains an address: fe80::1
     13/04/19 19:20:49 WARN hostess.Hostess$Map: (file pos: 0) Record only contains an address (with inline comment): 120.1.1.1
     13/04/19 19:20:49 WARN hostess.Hostess$Map: (file pos: 0) Record only contains an address (with inline comment): 120.1.1.1
     13/04/19 19:20:49 WARN hostess.Hostess$Map: (file pos: 0) Record only contains an address (with inline comment): fe80::1
     13/04/19 19:20:49 WARN hostess.Hostess$Map: (file pos: 0) Record only contains an address (with inline comment): fe80::1
     Tests run: 10, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.676 sec

     Results :

     Tests run: 10, Failures: 0, Errors: 0, Skipped: 0

     [INFO] ------------------------------------------------------------------------
     [INFO] BUILD SUCCESS
     [INFO] ------------------------------------------------------------------------
     [INFO] Total time: 2.165s
     [INFO] Finished at: Fri Apr 19 19:20:49 PDT 2013
     [INFO] Final Memory: 11M/171M
     [INFO] ------------------------------------------------------------------------

Running the Job
===============

Stage Input Files
-----------------

    $ ls -lh /tmp/hosts.*
    -rw-rw-r-- 1 hduser hadoop 281K Apr 19 13:58 /tmp/hosts.1
    -rw-rw-r-- 1 hduser hadoop 2.8M Apr 19 13:58 /tmp/hosts.2
    $ hadoop dfs -mkdir /user/hduser/hosts/input
    $ hadoop dfs -copyFromLocal /tmp/hosts.* /user/hduser/hosts/input

Start the Job
-------------

    $ hadoop jar /path/to/Hostess-1.0-SNAPSHOT.jar com.swijaya.hostess.Hostess /user/hduser/hosts/input /user/hduser/hosts/output
    [snip]

Collect Output
--------------

    $ hadoop dfs -cat /user/hduser/hosts/output/part-r-00000 > hosts
    $ ls -lh hosts
    -rw-r--r-- 1 hduser hadoop 3.0M Apr 19 19:13 hosts

