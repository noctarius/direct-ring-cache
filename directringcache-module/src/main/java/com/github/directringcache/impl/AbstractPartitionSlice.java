package com.github.directringcache.impl;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.directringcache.spi.PartitionSlice;

public abstract class AbstractPartitionSlice
    implements PartitionSlice
{

    private final AtomicBoolean lock = new AtomicBoolean( false );

    private final AtomicInteger aquired = new AtomicInteger( 0 );

    private final AtomicInteger freed = new AtomicInteger( 0 );

    final int index;

    AbstractPartitionSlice( int index )
    {
        this.index = index;
    }

    protected synchronized PartitionSlice lock()
    {
        if ( !lock.compareAndSet( false, true ) )
        {
            throw new IllegalStateException( "PartitionSlice already locked" );
        }
        if ( aquired.get() != freed.get() )
        {
            throw new IllegalStateException( "Not all aquires (" + aquired.get() + ") are freed (" + freed.get() + ")" );
        }
        aquired.incrementAndGet();
        return this;
    }

    protected synchronized PartitionSlice unlock()
    {
        if ( !lock.compareAndSet( true, false ) )
        {
            throw new IllegalStateException( "PartitionSlice not locked" );
        }
        freed.incrementAndGet();
        return this;
    }

    protected abstract void free();

}
