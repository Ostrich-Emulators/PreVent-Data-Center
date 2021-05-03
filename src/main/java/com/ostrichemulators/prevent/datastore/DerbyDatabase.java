/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ostrichemulators.prevent.datastore;

import com.ostrichemulators.prevent.WorkItem;
import com.ostrichemulators.prevent.WorkItem.WorkItemBuilder;
import com.ostrichemulators.prevent.Worklist;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ryan
 */
public class DerbyDatabase implements Database {

  private static final Logger LOG = LoggerFactory.getLogger( DerbyDatabase.class );
  private final Connection connection;

  public DerbyDatabase( Connection connection ) {
    this.connection = connection;
  }

  @Override
  public List<WorkItem> getAllWorkItems() throws DatastoreException {
    List<WorkItem> items = new ArrayList<>();
    try ( PreparedStatement ps = connection.prepareStatement( "SELECT uuid,bytes,containerid,started,finished,format,status,message,inputpath,outputpath FROM workitem" ) ) {
      try ( ResultSet rs = ps.executeQuery() ) {
        while ( rs.next() ) {
          /* WorkItem( Path file, String id, String containerId, LocalDateTime started,
        LocalDateTime finished, String type, long size, Path outputdir ) {*/
          String id = rs.getString( 1 );
          long size = rs.getLong( 2 );
          String container = rs.getString( 3 );
          Timestamp start = rs.getTimestamp( 4 );
          Timestamp end = rs.getTimestamp( 5 );
          String format = rs.getString( 6 );
          WorkItem.Status stat = WorkItem.Status.values()[rs.getInt( 7 )];
          String msg = rs.getString( 8 );
          Path inp = Path.of( rs.getString( 9 ) );
          Path outp = Path.of( rs.getString( 10 ) );

          WorkItemBuilder bldr = WorkItem.builder( inp )
                .bytes( size )
                .outdir( outp )
                .id( id )
                .status( stat )
                .message( msg )
                .format( format )
                .container( container );
          if ( null != start ) {
            bldr.start( start.toLocalDateTime() );
          }
          if ( null != end ) {
            bldr.finish( end.toLocalDateTime() );
          }

          items.add( bldr.build() );
        }
      }
    }
    catch ( SQLException x ) {
      throw new DatastoreException( x );
    }

    return items;
  }

  @Override
  public Collection<WorkItem> upsert( Collection<WorkItem> upserts ) throws DatastoreException {
    // update or insert all the items in the list
    try ( PreparedStatement getOld = connection.prepareStatement( "SELECT uuid,id FROM workitem" );
          PreparedStatement upd = connection.prepareStatement( "UPDATE workitem SET containerid=?,started=?,finished=?,format=?,status=?,message=? WHERE id=?" );
          PreparedStatement ins = connection.prepareStatement( "INSERT INTO workitem(uuid, bytes, containerid,started,finished,format,status,message,inputpath,outputpath) VALUES (?,?,?,?,?,?,?,?,?,?) " ) ) {
      Map<String, Integer> updlkp = new HashMap<>();
      try ( ResultSet rs = getOld.executeQuery() ) {
        while ( rs.next() ) {
          updlkp.put( rs.getString( 1 ), rs.getInt( 2 ) );
        }
      }

      // FIXME: do all this in a transaction
      List<WorkItem> updates = upserts.stream()
            .filter( wi -> updlkp.containsKey( wi.getId() ) )
            .collect( Collectors.toList() );
      List<WorkItem> inserts = upserts.stream()
            .filter( wi -> !updlkp.containsKey( wi.getId() ) )
            .collect( Collectors.toList() );

      for ( WorkItem wi : updates ) {
        //containerid=?,started=?,finished=?,format=?,status=?,message=? WHERE id=?
        upd.setString( 1, wi.getContainerId() );
        if ( null == wi.getStarted() ) {
          upd.setNull( 2, Types.TIMESTAMP );
        }
        else {
          upd.setTimestamp( 2, Timestamp.valueOf( wi.getStarted() ) );
        }
        if ( null == wi.getFinished() ) {
          upd.setNull( 3, Types.TIMESTAMP );
        }
        else {
          upd.setTimestamp( 3, Timestamp.valueOf( wi.getFinished() ) );
        }

        upd.setString( 4, wi.getType() );
        upd.setInt( 5, wi.getStatus().ordinal() );
        upd.setString( 6, wi.getMessage() );
        upd.setInt( 7, updlkp.get( wi.getId() ) );
        upd.execute();
      }

      for ( WorkItem wi : inserts ) {
        //uuid, bytes, containerid,started,finished,format,status,message,inputpath,outputpath
        ins.setString( 1, wi.getId() );
        ins.setLong( 2, wi.getBytes() );
        ins.setString( 3, wi.getContainerId() );
        if ( null == wi.getStarted() ) {
          ins.setNull( 4, Types.TIMESTAMP );
        }
        else {
          ins.setTimestamp( 4, Timestamp.valueOf( wi.getStarted() ) );
        }
        if ( null == wi.getFinished() ) {
          ins.setNull( 5, Types.TIMESTAMP );
        }
        else {
          ins.setTimestamp( 5, Timestamp.valueOf( wi.getFinished() ) );
        }
        ins.setString( 6, wi.getType() );
        ins.setInt( 7, wi.getStatus().ordinal() );
        ins.setString( 8, wi.getMessage() );
        ins.setString( 9, wi.getPath().toString() );
        ins.setString( 10, wi.getOutputPath().toString() );
        ins.execute();
      }
      return inserts;
    }
    catch ( SQLException x ) {
      throw new DatastoreException( x );
    }
  }

  static void checkAndCreate( Connection c ) throws IOException, SQLException {
    try ( BufferedReader create = new BufferedReader( new InputStreamReader( Worklist.class.getResourceAsStream( "/create.sql" ) ) ) ) {
      String allsql = create.lines()
            .map( str -> str.replaceAll( "--.*", "" ) )
            .reduce( "", ( prev, next ) -> prev + next );
      for ( String sql : allsql.split( ";" ) ) {
        try ( Statement s = c.createStatement() ) {
          s.execute( sql );
        }
        catch ( SQLException x ) {
          // X0Y32 means table already exists
          // 23505 is a duplicate value (for the status lookup table)
          if ( !( "X0Y32".equals( x.getSQLState() ) || "23505".equals( x.getSQLState() ) ) ) {
            LOG.info( "problem with SQL: {}", sql );
            throw x;
          }
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    try {
      connection.close();
    }
    catch ( SQLException x ) {
      // don't care
    }
  }
}
