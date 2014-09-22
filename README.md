Parallel GZIPOutputStream and GZIPInputStream
=============================================

This library contains a parallelized GZIP implementation which is
intended as a drop-in replacement for the standard java.util.zip
classes. It is a pure Java equivalent of the pigz parallel compresssor.

The performance of ParallelGZIPOutputStream is approximately
twice as good as the standard GZIPOutputStream. Currently,
ParallelGZIPInputStream is a subclass of the standard GZIPInputStream,
but parallelism may be added in future.

API Documentation
=================

The [JavaDoc API](http://shevek.github.io/parallelgzip/docs/javadoc/)
is available.


References
==========

* http://zlib.net/pigz/pigz.pdf
* http://www.gzip.org/zlib/rfc-gzip.html

Credits
=======

I needed this at work, but I was inspired to
publish it by Paul Eggert's CS131 coursework at
http://www.cs.ucla.edu/classes/fall11/cs131/hw/hw3.html - I wonder
what grade I will get.

