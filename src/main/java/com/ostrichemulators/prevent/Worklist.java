/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ostrichemulators.prevent;

import com.ostrichemulators.prevent.WorkItem.WorkItemBuilder;
import com.ostrichemulators.prevent.datastore.Database;
import com.ostrichemulators.prevent.datastore.DatastoreException;
import com.ostrichemulators.prevent.datastore.DatastoreFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
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
public class Worklist implements AutoCloseable {

  private final Database db;

  private static final Logger LOG = LoggerFactory.getLogger( Worklist.class );
  private static final Map<String, String> EXT_TYPE_LKP = Map.of(
        "medi", "tdms",
        "xml", "stpxml",
        "tdms", "tdms",
        "mat", "mat5",
        "mat4", "mat4",
        "mat73", "mat73",
        "stp", "stp" );

  private final List<WorkItem> items = new ArrayList<>();

  private Worklist( Database c ) {
    db = c;
  }

  public static Worklist open( Path saveloc ) throws DatastoreException {
    return new Worklist( DatastoreFactory.open( saveloc ) );
  }

  public List<WorkItem> list() {
    try {
      items.clear();
      items.addAll( db.getAllWorkItems() );
    }
    catch ( DatastoreException x ) {
      LOG.error( "", x );
    }

    return Collections.unmodifiableList( items );
  }

  public void upsert( List<WorkItem> upserts ) throws DatastoreException {
    items.addAll( db.upsert( items ) );
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

  @Override
  public void close() throws Exception {
    try {
      db.close();
    }
    catch ( DatastoreException x ) {
      // don't care
    }
  }
}
