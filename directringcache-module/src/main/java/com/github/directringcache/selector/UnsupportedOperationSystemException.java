package com.github.directringcache.selector;

@SuppressWarnings( "serial" )
public class UnsupportedOperationSystemException
    extends RuntimeException
{

    UnsupportedOperationSystemException()
    {
        super();
    }

    UnsupportedOperationSystemException( String message, Throwable cause )
    {
        super( message, cause );
    }

    UnsupportedOperationSystemException( String message )
    {
        super( message );
    }

    UnsupportedOperationSystemException( Throwable cause )
    {
        super( cause );
    }

}
