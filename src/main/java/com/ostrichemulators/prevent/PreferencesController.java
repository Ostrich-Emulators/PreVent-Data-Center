package com.ostrichemulators.prevent;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ResourceBundle;
import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.OverrunStyle;
import javafx.scene.control.RadioButton;
import javafx.scene.control.Spinner;
import javafx.scene.control.SpinnerValueFactory;
import javafx.scene.control.SpinnerValueFactory.ListSpinnerValueFactory;
import javafx.stage.DirectoryChooser;
import javafx.stage.Window;
import javafx.util.StringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import javafx.application.Platform;

public class PreferencesController implements Initializable {

  private static final Logger LOG = LoggerFactory.getLogger( PreferencesController.class );

  @FXML
  private CheckBox nativestp;

  @FXML
  private CheckBox usephilips;

  @FXML
  private CheckBox removecontainers;

  @FXML
  private Spinner<Integer> dockercnt;

  @FXML
  private Spinner<Integer> durationtimer;

  @FXML
  private Label outputlbl;

  @FXML
  private Label loglbl;

  @FXML
  private RadioButton dockerconverter;

  @FXML
  private RadioButton nativeconverter;

  @FXML
  void saveconfig() {
    LOG.debug( "saving preferences" );
    App.converter.setMaxRunners( dockercnt.getValue() );
    dockercnt.setValueFactory( new SpinnerValueFactory.IntegerSpinnerValueFactory( 1,
          10, dockercnt.getValue() ) );

    App.prefs
          .setMaxConversionMinutes( durationtimer.getValue() )
          .setNativeStp( nativestp.isSelected() )
          .setMaxDockerCount( dockercnt.getValue() )
          .setStpPhilips( usephilips.isSelected() )
          .setRemoveDockerOnSuccess( removecontainers.isSelected() )
          .setLogPath( Paths.get( loglbl.getText() ) );
    if ( !"From Input".equals( outputlbl.getText() ) ) {
      App.prefs.setOutputPath( Paths.get( outputlbl.getText() ) );
    }
  }

  private void loadPrefs() {
    nativestp.setSelected( App.prefs.useNativeStp() );
    usephilips.setSelected( App.prefs.isStpPhilips() );

    if ( App.prefs.useNativeConverter() ) {
      nativeconverter.setSelected( true );
    }

    removecontainers.setSelected( App.prefs.removeDockerOnSuccess() );
    App.converter.setMaxRunners( App.prefs.getMaxDockerCount() );
    dockercnt.setValueFactory( new SpinnerValueFactory.IntegerSpinnerValueFactory( 1,
          10, App.converter.getMaxRunners() ) );

    outputlbl.setTextOverrun( OverrunStyle.CENTER_ELLIPSIS );
    Path outpath = App.prefs.getOutputPath();
    outputlbl.setText( null == outpath ? "From Input" : outpath.toString() );

    if ( null != outpath && !Files.exists( outpath ) ) {
      LOG.info( "creating output directory: {}", outpath );

      boolean created = outpath.toFile().mkdirs();
      if ( !created ) {
        Alert alert = new Alert( Alert.AlertType.ERROR );
        alert.setTitle( "Docker Images Missing" );
        alert.setHeaderText( "Docker is not Initialized Properly" );
        ButtonType exitbtn = new ButtonType( "Cancel", ButtonBar.ButtonData.CANCEL_CLOSE );
        alert.getButtonTypes().setAll( exitbtn );
        alert.setContentText( "Docker may not be started, or the ry99/prevent image could not be pulled.\nOn Windows, Docker must be listening to port 2375.\nContinuing with Native Converter" );

      }
    }

    loglbl.setTextOverrun( OverrunStyle.CENTER_ELLIPSIS );
    Path logdirpath = App.prefs.getLogPath();
    loglbl.setText( logdirpath.toString() );

    ListSpinnerValueFactory<Integer> vfac = new SpinnerValueFactory.ListSpinnerValueFactory<>(
          FXCollections.observableArrayList( 10, 30, 60, 180, 480, Integer.MAX_VALUE ) );
    vfac.setConverter( new StringConverter<Integer>() {
      @Override
      public String toString( Integer t ) {
        if ( t <= 60 ) {
          return String.format( "%d minutes", t );
        }
        if ( t < Integer.MAX_VALUE ) {
          return String.format( "%d hours", t / 60 );
        }
        return "Unlimited";
      }

      @Override
      public Integer fromString( String string ) {
        for ( Integer i : vfac.getItems() ) {
          if ( toString( i ).equals( string ) ) {
            return i;
          }
        }
        return Integer.MAX_VALUE;
      }
    } );
    vfac.setValue( Integer.MAX_VALUE );
    durationtimer.setValueFactory( vfac );
  }

  @Override
  public void initialize( URL url, ResourceBundle rb ) {
    removecontainers.disableProperty().bind( nativeconverter.selectedProperty() );

    loadPrefs();

    Path outpath = App.prefs.getOutputPath();
    if ( null != outpath && !Files.exists( outpath ) ) {
      LOG.info( "creating output directory: {}", outpath );

      boolean created = outpath.toFile().mkdirs();
      if ( !created ) {
        Alert alert = new Alert( Alert.AlertType.ERROR );
        alert.setTitle( "Cannot create output directory" );
        alert.setHeaderText( "Output path: " + outpath.toString() + " does not exist and could not be created" );
        ButtonType exitbtn = new ButtonType( "Okay", ButtonBar.ButtonData.CANCEL_CLOSE );
        alert.getButtonTypes().setAll( exitbtn );
        alert.setContentText( outpath + " does not exist and could not be created\nOutput files will not be generated\nPlease set the \"Output Path\" preference before converting files" );
        alert.showAndWait();
      }
    }

    boolean natives = nativeconverter.isSelected();

    if ( App.converter.useNativeConverters( natives ) ) {
      LOG.debug( "Converter is ready!" );
    }
    else if ( !natives ) {
      dockerconverter.setDisable( true );
      nativeconverter.setSelected( true );

      Alert alert = new Alert( Alert.AlertType.ERROR );
      alert.setTitle( "Docker Images Missing" );
      alert.setHeaderText( "Docker is not Initialized Properly" );
      ButtonType exitbtn = new ButtonType( "Cancel", ButtonBar.ButtonData.CANCEL_CLOSE );
      alert.getButtonTypes().setAll( exitbtn );
      alert.setContentText( "Docker may not be started, or the ry99/prevent image could not be pulled.\nOn Windows, Docker must be listening to port 2375.\nContinuing with Native Converter" );
      alert.showAndWait();

      if ( !App.converter.useNativeConverters( natives ) ) {
        alert = new Alert( Alert.AlertType.ERROR );
        alert.setTitle( "No Converters initialized" );
        alert.setHeaderText( "The fallback (Native) Converter did not initialize properly." );
        alert.getButtonTypes().setAll( exitbtn );
        alert.setContentText( "Are you on a supported platform? Windows and Linux are supported at this time." );
        alert.showAndWait();
        Platform.exit();
      }
    }
  }

  @FXML
  void selectOutputDir() {
    DirectoryChooser chsr = new DirectoryChooser();
    chsr.setTitle( "Select Output Directory" );

    Path outdir = App.prefs.getOutputPath();
    if ( null != outdir && Files.exists( outdir ) ) {
      chsr.setInitialDirectory( outdir.toFile() );
    }
    Window window = nativestp.getScene().getWindow();

    File dir = chsr.showDialog( window );
    outputlbl.setText( dir.getAbsolutePath() );
  }

  @FXML
  void setOutputFromInput() {
    outputlbl.setText( "From Input" );
  }

  @FXML
  void selectLogDir() {
    DirectoryChooser chsr = new DirectoryChooser();
    chsr.setTitle( "Select Log Directory" );

    Path outdir = App.prefs.getLogPath();
    chsr.setInitialDirectory( outdir.toFile() );
    Window window = nativestp.getScene().getWindow();

    File dir = chsr.showDialog( window );
    loglbl.setText( dir.getAbsolutePath() );
  }
}
