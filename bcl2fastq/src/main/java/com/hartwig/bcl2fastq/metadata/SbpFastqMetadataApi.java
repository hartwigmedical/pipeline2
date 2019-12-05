package com.hartwig.bcl2fastq.metadata;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.pipeline.jackson.ObjectMappers;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SbpFastqMetadataApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(SbpFastqMetadataApi.class);
    private static final String FLOWCELLS = "flowcells";
    private static final String SAMPLES = "samples";
    private final WebTarget target;
    private final ObjectMapper objectMapper = ObjectMappers.get();

    private SbpFastqMetadataApi(final WebTarget target) {
        this.target = target;
    }

    public SbpFlowcell getFlowcell(String name) {
        try {
            return findOne(api().path(FLOWCELLS).queryParam("name", name).request(), new TypeReference<List<SbpFlowcell>>() {
            }).orElseThrow(() -> new IllegalArgumentException(String.format(
                    "No flowcell found for experiment name [%s] Has it been registered with the SBP API?",
                    name)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SbpSample getOrCreate(String barcode, String submission) {
        try {
            return findOne(samples().queryParam("barcode", barcode).request(), new TypeReference<List<SbpSample>>() {
            }).orElseGet(() -> {
                SbpSample sample = SbpSample.builder().barcode(barcode).status("Unregistered").submission(submission).build();
                try {
                    Response response = samples().request()
                            .post(Entity.entity(objectMapper.writeValueAsString(sample), MediaType.APPLICATION_JSON_TYPE));
                    if (response.getStatus() == 200) {
                        return getOrCreate(barcode, submission);
                    } else {
                        throw new RuntimeException(String.format("Unable to pose new sample [%s] api returned status [%s] and message [%s]",
                                barcode,
                                response.getStatus(),
                                response.readEntity(String.class)));
                    }
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private WebTarget samples() {
        return api().path(SAMPLES);
    }

    public SbpFlowcell updateFlowcell(SbpFlowcell flowcell) {
        try {
            Response response = api().path(FLOWCELLS)
                    .path(flowcell.id())
                    .request()
                    .build("PATCH", Entity.entity(objectMapper.writeValueAsString(flowcell), MediaType.APPLICATION_JSON_TYPE))
                    .invoke();
            LOGGER.info("Patching flowcell [{}] complete with status [{}]", flowcell.name(), response.getStatus());
            return getFlowcell(flowcell.name());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> Optional<T> findOne(final Invocation.Builder request, final TypeReference<List<T>> reference) throws IOException {
        return objectMapper.<List<T>>readValue(returnOrThrow(request.get()), reference).stream().findFirst();
    }

    private WebTarget api() {
        return target.path("hmf").path("v1");
    }

    private String returnOrThrow(final Response response) {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(String.class);
        }
        throw error(response);
    }

    private RuntimeException error(final Response response) {
        return new RuntimeException(format("Received an error status result [%s] of SBP Api at [%s]",
                response.getStatus(),
                target.getUri()));
    }

    public static SbpFastqMetadataApi newInstance(final String url) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
        return new SbpFastqMetadataApi(ClientBuilder.newBuilder().withConfig(clientConfig).build().target(url));
    }
}
