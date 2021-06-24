package com._4paradigm.dataimporter;

import org.junit.Assert;
import org.junit.Test;

public class WildcardURITest {

    @Test
    public void test() {
        String path = "hdfs://host/testdata/20180[8-9]*";
        WildcardURI wildcardURI = new WildcardURI(path);
        Assert.assertEquals("/testdata/20180[8-9]*", wildcardURI.getPath());

        path = "hdfs://host/testdata/2018+ 0[8-9]*";
        wildcardURI = new WildcardURI(path);
        Assert.assertEquals("/testdata/2018+ 0[8-9]*", wildcardURI.getPath());

        path = "hdfs://host/testdata/2018-01-01 00%3A00%3A00";
        wildcardURI = new WildcardURI(path);
        Assert.assertEquals("/testdata/2018-01-01 00%3A00%3A00", wildcardURI.getPath());

        path = "hdfs://host/testdata/2018-01-01   00*";
        wildcardURI = new WildcardURI(path);
        Assert.assertEquals("/testdata/2018-01-01   00*", wildcardURI.getPath());

        path = "hdfs://host/testdata/2018-01-01#123#";
        wildcardURI = new WildcardURI(path);
        Assert.assertEquals("/testdata/2018-01-01#123#", wildcardURI.getPath());

        path = "hdfs://host/testdata/2018-01-01#123 +#*";
        wildcardURI = new WildcardURI(path);
        Assert.assertEquals("/testdata/2018-01-01#123 +#*", wildcardURI.getPath());
    }

}
