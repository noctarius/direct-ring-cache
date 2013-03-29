package com.github.directringcache.io;

import java.io.DataOutputStream;

import com.github.directringcache.PartitionBuffer;

public class PartitionDataOutputStream
    extends DataOutputStream
{

    public PartitionDataOutputStream( PartitionBuffer partitionBuffer )
    {
        super( new PartitionOutputStream( partitionBuffer ) );
    }

}
