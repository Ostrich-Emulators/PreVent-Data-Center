/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ostrichemulators.prevent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ostrichemulators.prevent.WorkItem.WorkItemBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ryan
 */
public class Worklist {

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

  private static ObjectMapper objmap;

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

  public List<WorkItem> list(){
    return new ArrayList<>();
  }

  public static List<WorkItem> open2( Path saveloc ) throws IOException {
    List<WorkItem> list = new ArrayList<>();
    if ( null == objmap ) {
      objmap = new ObjectMapper();
      objmap.registerModule( new JavaTimeModule() );
      objmap.configure( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false );
      objmap.enable( SerializationFeature.INDENT_OUTPUT );
    }

    File file = saveloc.toFile();
    if ( file.exists() ) {
      WorkItem[] reads = objmap.readValue( saveloc.toFile(), WorkItem[].class );
      list.addAll( Arrays.asList( reads ) );
    }
    return list;
  }

  public static void save( List<WorkItem> items ) throws IOException {
//    if ( !savedloc.getParent().toFile().exists() ) {
//      savedloc.getParent().toFile().mkdirs();
//    }
//    objmap.writeValue( savedloc.toFile(), items );
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
          if ( !"X0Y32".equals( x.getSQLState() ) ) {
            LOG.info( "problem with SQL: " + sql );
            throw x;
          }
        }
      }
    }
  }
}
