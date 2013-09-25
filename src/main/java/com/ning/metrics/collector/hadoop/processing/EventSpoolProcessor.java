package com.ning.metrics.collector.hadoop.processing;

import java.io.File;
import java.io.IOException;

public interface EventSpoolProcessor
{
    public void processEventFile(final File file, final String hadoopOutputPath) throws IOException;
    public void close();
    public String getProcessorName();
}
