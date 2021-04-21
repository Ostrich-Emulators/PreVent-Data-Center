/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ostrichemulators.prevent;

import com.ostrichemulators.prevent.WorkItem.Status;
import com.ostrichemulators.prevent.conversion.Converter.LogType;
import com.ostrichemulators.prevent.conversion.Logable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author ryan
 */
public class Conversion {

  private Duration maxlifespan;
  private WorkItem item;
  private Path xmlpath;
  private Path logdir;
  private boolean compresslogs = true;
  private final List<WorkItemStateChangeListener> listeners = new ArrayList<>();
  private Logable worker;

  private Conversion() {
  }

  public static ConversionBuilder builder( WorkItem item ) {
    return new Conversion().new ConversionBuilder().item( item );
  }

  public Duration getMaxLifespan() {
    return maxlifespan;
  }

  public WorkItem getItem() {
    return item;
  }

  public Path getXmlPath() {
    return xmlpath;
  }

  public boolean isStatus( Status s ) {
    return s.equals( item.getStatus() );
  }

  public boolean runOverlong() {
    return LocalDateTime.now().isAfter( item.getStarted().plus( maxlifespan ) );
  }

  public void setLogable( Logable l ) {
    this.worker = l;
  }

  public Logable getLogable() {
    return this.worker;
  }

  public boolean needsStpToXml() {
    return ( Objects.isNull( xmlpath )
             ? false
             : !Files.exists( xmlpath ) );
  }

  public Path getLogDir() {
    return logdir;
  }

  public boolean isCompressLogs() {
    return compresslogs;
  }

  public void tellListeners() {
    listeners.stream().forEach( l -> l.itemChanged( item ) );
  }

  /**
   * Gets the location of the saved log files, whether or not they exist
   *
   * @param type
   * @param err
   * @return
   */
  public Path getSavedLog( LogType type, boolean err ) {
    Path datadir = getLogDir();
    String gz = ( err
                  ? "stderr.gz"
                  : "stdout.gz" );
    return datadir.resolve( type + "-" + gz );
  }

  /**
   * Gets a Reader for the log files for this item. If the item is currently
   * being preprocessed or running, this function returns the stdout/stderr of
   * the running process. If the item is not running, it returns a Reader to the
   * saved logs, if they exist
   *
   * @param type
   * @param err
   * @return
   * @throws IOException if the desired log does not exist
   */
  public Reader getLogReader( LogType type, boolean err ) throws IOException {
    // if we're currently running, use the "live" output
    if ( ( isStatus( Status.PREPROCESSING ) && LogType.STP == type )
          || ( isStatus( Status.RUNNING ) && LogType.CONVERSION == type ) ) {
      return Files.newBufferedReader( err
                                      ? getLogable().getErr()
                                      : getLogable().getOut() );
    }

    Path path = getSavedLog( type, err );
    File datadir = path.getParent().toFile();
    if ( !( datadir.exists() && datadir.isDirectory() ) ) {
      throw new IOException( "Cannot locate log directory (or not a directory): " + datadir );
    }

    return new InputStreamReader( new GZIPInputStream( new FileInputStream( path.toFile() ) ) );
  }

  public class ConversionBuilder {

    private ConversionBuilder() {
    }

    public ConversionBuilder maxRuntime( Duration d ) {
      maxlifespan = d;
      return this;
    }

    public ConversionBuilder item( WorkItem i ) {
      item = i;
      return this;
    }

    public ConversionBuilder withXml( Path path ) {
      xmlpath = path;
      return this;
    }

    public ConversionBuilder withLogsIn( Path logs ) {
      logdir = logs;
      return this;
    }

    public ConversionBuilder compressLogs( boolean compress ) {
      compresslogs = compress;
      return this;
    }

    public ConversionBuilder listener( WorkItemStateChangeListener l ) {
      listeners.add( l );
      return this;
    }

    public ConversionBuilder setListeners( Collection<WorkItemStateChangeListener> ls ) {
      listeners.clear();
      listeners.addAll( ls );
      return this;
    }

    public Conversion build() {
      return Conversion.this;
    }
  }
}
