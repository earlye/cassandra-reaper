package com.spotify.reaper.resources;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperApplicationConfiguration;

@Path("/configuration")
@Produces(MediaType.APPLICATION_JSON)
public class ConfigurationResource {

	private final AppContext context;

	public ConfigurationResource(AppContext context) {
		this.context = context;
	}

	@GET
	public Response getConfiguration() throws WebApplicationException {
		try {
			String configJson = context.objectMapper.writeValueAsString(context.config);
			ReaperApplicationConfiguration config = context.objectMapper.readValue(configJson,
					ReaperApplicationConfiguration.class);

			// Hide passwords
			if (null != config.getJmxAuth()) {
				config.getJmxAuth().setPassword(null);
			}

			if (null != config.getDataSourceFactory()) {
				config.getDataSourceFactory().setPassword(null);
			}

			return Response.ok(config).build();
		} catch (IOException e) {
			e.printStackTrace();
			throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
		}
	}

	@PUT
	@Path("maxPendingCompactions")
	public Response putMaxPendingCompactions(ValueHolder<Integer> valueHolder) {
		context.config.setMaxPendingCompactions(valueHolder.value);
		return Response.ok(valueHolder).build();
	}

	@GET
	@Path("maxPendingCompactions")
	public Response putMaxPendingCompactions() {
		ValueHolder<Integer> valueHolder = new ValueHolder<>();
		valueHolder.value = context.config.getMaxPendingCompactions();
		return Response.ok(valueHolder).build();
	}

}
