/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ostrichemulators.prevent;

import com.ostrichemulators.prevent.WorkItem.Status;
import com.ostrichemulators.prevent.WorkItem.WorkItemBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ryan
 */
public class Worklist implements AutoCloseable {

  private final Connection conn;

  private static final Logger LOG = LoggerFactory.getLogger( Worklist.class );
  private static final Map<String, String> EXT_TYPE_LKP = Map.of(
        "medi", "tdms",
        "xml", "stpxml",
        "tdms", "tdms",
        "mat", "mat5",
        "mat4", "mat4",
        "mat73", "mat73",
        "stp", "stp" );

  //private static ObjectMapper objmap;
  private final List<WorkItem> items = new ArrayList<>();

  private Worklist( Connection c ) {
    conn = c;
  }

  public static Worklist open( Path saveloc ) throws IOException {
    try {
      Class.forName( "org.apache.derby.jdbc.EmbeddedDriver" ).getDeclaredConstructor().newInstance();
      Connection conn = DriverManager.getConnection( "jdbc:derby:" + saveloc.toAbsolutePath() + ";create=true" );

      checkAndCreate( conn );

      return new Worklist( conn );
    }
    catch ( Exception x ) {
      throw new IOException( x );
    }
  }

  public List<WorkItem> list() {
    try ( PreparedStatement ps = conn.prepareStatement( "SELECT uuid,bytes,containerid,started,finished,format,status,message,inputpath,outputpath FROM workitem" ) ) {
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
          Status stat = Status.values()[rs.getInt( 7 )];
          String msg = rs.getString( 8 );
          Path inp = Path.of( rs.getString( 9 ) );
          Path outp = Path.of( rs.getString( 10 ) );

          WorkItem item = new WorkItem( inp, id, container, ( null == start ? null : start.toLocalDateTime() ),
                ( null == end ? null : end.toLocalDateTime() ), format, size, outp );
          item.setStatus( stat );
          item.setMessage( msg );
          items.add( item );
        }
      }
    }
    catch ( SQLException x ) {
      LOG.error( "", x );
    }

    return Collections.unmodifiableList( items );
  }

//  public static List<WorkItem> open2( Path saveloc ) throws IOException {
//    List<WorkItem> list = new ArrayList<>();
//    if ( null == objmap ) {
//      objmap = new ObjectMapper();
//      objmap.registerModule( new JavaTimeModule() );
//      objmap.configure( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false );
//      objmap.enable( SerializationFeature.INDENT_OUTPUT );
//    }
//
//    File file = saveloc.toFile();
//    if ( file.exists() ) {
//      WorkItem[] reads = objmap.readValue( saveloc.toFile(), WorkItem[].class );
//      list.addAll( Arrays.asList( reads ) );
//    }
//    return list;
//  }
  public void upsert( List<WorkItem> upserts ) throws IOException {
    // update or insert all the items in the list
    try ( PreparedStatement getOld = conn.prepareStatement( "SELECT uuid,id FROM workitem" );
          PreparedStatement upd = conn.prepareStatement( "UPDATE workitem SET containerid=?,started=?,finished=?,format=?,status=?,message=? WHERE id=?" );
          PreparedStatement ins = conn.prepareStatement( "INSERT INTO workitem(uuid, bytes, containerid,started,finished,format,status,message,inputpath,outputpath) VALUES (?,?,?,?,?,?,?,?,?,?) " ) ) {
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

        items.set( items.indexOf( wi ), wi ); // is this necessary?
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
        items.add( wi );
      }
    }
    catch ( SQLException x ) {
      throw new IOException( x );
    }

  }

  public static Optional<WorkItem> from( Path p, boolean nativestp ) {
    File f = p.toFile();
    final Path outdir = App.prefs.getOutputPath();

    if ( f.canRead() ) {
      if ( f.isDirectory() ) {
        // check if this directory is WFDB, DWC, or ZL

        // DWC
        File[] inners = f.listFiles( fname -> FilenameUtils.isExtension( fname.getName().toLowerCase(), "info" ) );
        if ( inners.length > 0 ) {
          Optional<WorkItem> wi = from( inners[0].toPath(), nativestp );
          wi.ifPresent( i -> {
            i.setBytes( FileUtils.sizeOfDirectory( f ) );
            i.setType( "dwc" );
          } );
          return wi;
        }

        // WFDB
        inners = f.listFiles( fname -> FilenameUtils.isExtension( fname.getName().toLowerCase(), "hea" ) );
        if ( inners.length > 0 ) {
          Optional<WorkItem> wi = from( inners[0].toPath(), nativestp );
          wi.ifPresent( i -> {
            i.setBytes( FileUtils.sizeOfDirectory( f ) );
            i.setType( "wfdb" );
          } );
          return wi;
        }

        // ZL
        inners = f.listFiles( fname -> FilenameUtils.isExtension( fname.getName().toLowerCase(), "gzip" ) );
        if ( inners.length > 0 ) {
          //return Optional.of( new WorkItem( p, DigestUtils.md5Hex( p.toAbsolutePath().toString() ), null, null, null, "zl" ) );
          // ignore checksums for now
          return Optional.of( WorkItem.builder( p )
                .type( "zl" )
                .bytes( FileUtils.sizeOfDirectory( f ) )
                .outdir( outdir )
                .build() );
        }
      }
      else if ( !FilenameUtils.isExtension( p.getFileName().toString().toLowerCase(), "hdf5" ) ) {
        WorkItemBuilder builder = WorkItem.builder( p )
              .outdir( outdir )
              .bytes( FileUtils.sizeOf( p.toFile() ) );

        if ( FilenameUtils.isExtension( p.getFileName().toString().toLowerCase(), "stp" ) && !nativestp ) {
          builder.type( "stpxml" );
        }
        else {
          builder.type( EXT_TYPE_LKP.getOrDefault( FilenameUtils.getExtension( p.getFileName().toString().toLowerCase() ), "unknown" ) );
        }

        return Optional.of( builder.build() );
      }
    }
    return Optional.empty();
  }

  public static List<WorkItem> recursively( Path p, boolean nativestp ) {
    List<WorkItem> items = new ArrayList<>();
    File f = p.toFile();
    if ( f.canRead() ) {
      if ( f.isDirectory() ) {
        // if we have a DWC, WFDB, or ZL directory, we can't recurse...
        // but if we have anything else, then recurse and add whatever we find.

        // DWC
        File[] inners = f.listFiles( fname -> FilenameUtils.isExtension( fname.getName().toLowerCase(), "info" ) );
        if ( inners.length > 0 ) {
          from( inners[0].toPath(), nativestp ).ifPresent( wi -> {
            wi.setBytes( FileUtils.sizeOfDirectory( f ) );
            wi.setType( "dwc" );
            items.add( wi );
          } );
        }
        else {
          // WFDB
          inners = f.listFiles( fname -> FilenameUtils.isExtension( fname.getName().toLowerCase(), "hea" ) );
          if ( inners.length > 0 ) {
            from( inners[0].toPath(), nativestp ).ifPresent( wi -> {
              wi.setBytes( FileUtils.sizeOfDirectory( f ) );
              wi.setType( "wfdb" );
              items.add( wi );
            } );
          }
          else {
            // ZL
            inners = f.listFiles( fname -> FilenameUtils.isExtension( fname.getName().toLowerCase(), "gzip" ) );
            if ( inners.length > 0 ) {
              from( p, nativestp ).ifPresent( wi -> {
                wi.setBytes( FileUtils.sizeOfDirectory( f ) );
                wi.setType( "zl" );
                items.add( wi );
              } );
            }
            else {
              // f is a directory, so add all files we find there, and recurse
              // into all subdirectories
              for ( File sub : f.listFiles() ) {
                if ( sub.isDirectory() ) {
                  items.addAll( recursively( sub.toPath(), nativestp ) );
                }
                else {
                  from( sub.toPath(), nativestp ).ifPresent( wi -> items.add( wi ) );
                }
              }
            }
          }
        }
      }
      else {
        from( p, nativestp ).ifPresent( wi -> items.add( wi ) );
      }
    }

    return items;
  }

  private static void checkAndCreate( Connection c ) throws IOException, SQLException {
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
      conn.close();
    }
    catch ( SQLException x ) {
      // don't care
    }
  }
}
