# A Flink application project using Scala and SBT

To run and test your application use SBT invoke: 'sbt run'

In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations.
Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the "Use classpath of module" dropbox. 

## Additions (by Andreas Markmann)

### SocketTextStreamTransformations.scala

Demonstrates simple transformations like `map`, `filter`, `split`, `union`.
To send input while the program is running, start `nc -lk 9999` on a command line and input uppercase and lowercase words.

### SocketTextStreamJoin.scala

Demonstrates join on a short window. To send input while the program is running,
run `nc -lk 9998` and `nc -lk 9999` in separate command-line
windows and enter whitespace-separated key-value pairs, for example:

| left input | right input |
|   :---:    |    :---:    |
|    a b     |     a 1     |
|    a c     |     a 2     |
|    b d     |     b 3     |

