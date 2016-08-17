package com.spotify.reaper.storage.postgresql;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

import javax.naming.directory.InvalidAttributesException;

import org.skife.jdbi.v2.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperApplication;
import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.storage.PostgresStorage;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;

public class CommandSetupSchema extends ConfiguredCommand<ReaperApplicationConfiguration>{

	//private AppContext context;

	static public String getReaperDbSql() throws IOException {
		
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		InputStream is = classLoader.getResourceAsStream("db/reaper_db.sql");
		
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int length;
		while ((length = is.read(buffer)) != -1) {
		    result.write(buffer, 0, length);
		}
		return result.toString("UTF-8");
	}
	
	
	public CommandSetupSchema() {
		super("schema", "Build database schema");
	}

	static Logger log = LoggerFactory.getLogger(CommandSetupSchema.class);
	
	@Override
	protected void run(Bootstrap<ReaperApplicationConfiguration> bootstrap, Namespace namespace,
			ReaperApplicationConfiguration configuration) throws Exception {
		// Unfortunately, DropWizard doesn't pass the same things to ConfiguredCommand.run that 
		// it passes to Application.run, so we have to apply a little spackle here:
        final Environment environment = new Environment(bootstrap.getApplication().getName(),
                bootstrap.getObjectMapper(),
                bootstrap.getValidatorFactory().getValidator(),
                bootstrap.getMetricRegistry(),
                bootstrap.getClassLoader());

		ReaperApplication.checkConfiguration(configuration);
		
		if (!"database".equalsIgnoreCase(configuration.getStorageType())) {
			throw new InvalidAttributesException("Configuration must specify storageType: database");
		}
		
		PostgresStorage storage = new PostgresStorage(configuration, environment);
		if (!storage.isStorageConnected()) {
			throw new IOException("Could not connect to Postgres");
		}
		
		String reaperDbSql = getReaperDbSql();
		//log.debug(reaperDbSql);
		
		String commands[] = reaperDbSql.split(";");
		
		for (String command:commands) {
			log.info("Executing:" + command);
			Handle handle = storage.getHandle();
			handle.execute(command);
			handle.close();
		}
	}

}
