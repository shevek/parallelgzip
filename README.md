Parallel GZIPOutputStream and GZIPInputStream
=============================================

This library contains a parallelized GZIP implementation which is a
high performance drop-in replacement for the standard java.util.zip
classes. It is a pure Java equivalent of the pigz parallel compresssor.

The performance of ParallelGZIPOutputStream is excellent: it scales
linearly with the number of cores, and spends 95% of all thread time
in the native compression routines on 24 and c32-core Xeon systems.

ParallelGZIPOutputStream has exactly the same memory contract as
GZIPOutputStream: it extends FilterOutputStream. If a single thread
writes data into the compressor, it will write compressed data to the
underlying output stream on the same thread without any externally
visible synchronization or flush calls. The user never needs to know
or change anything as a result of the parallelism.

Currently, ParallelGZIPInputStream is a subclass of the standard
GZIPInputStream, but parallelism may be added in future.

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

