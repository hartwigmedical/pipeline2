package com.hartwig.patient.input;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.hartwig.patient.Patient;
import com.hartwig.support.test.Resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

public class PatientReaderTest {

    private static final String PATIENT = "CPCT12345678";
    private static final String PATIENTS = "patients/";
    private static final String SINGLE_SAMPLE = Resources.testResource(PATIENTS + "singleSample");
    private static final String REFERENCE_AND_TUMOUR = Resources.testResource(PATIENTS + "referenceAndTumor");
    private static final String INFER_PATIENT_ID_AMBIGUOUS_DIRECTORIES =
            Resources.testResource(PATIENTS + "inferPatient/ambiguousDirectories");
    private static final String INFER_PATIENT_ID_PATIENT = Resources.testResource(PATIENTS + "inferPatient/patient");
    private static final String MULTIPLE_FLOWCELLS = Resources.testResource("patients/multipleFlowcells");

    @Test(expected = FileNotFoundException.class)
    public void nonExistentDirectoryThrowsIllegalArgument() throws Exception {
        reader("/not/a/directory", "");
    }

    @Test
    public void onlyFilesReturnsSingleSampleMode() throws Exception {
        Patient victim = reader(SINGLE_SAMPLE, PATIENT);
        assertThat(victim.reference()).isNotNull();
        assertThat(victim.reference().lanes()).hasSize(1);
        assertThat(victim.maybeTumor()).isEmpty();
    }

    @Test
    public void twoCorrectlyNamedSampleDirectoriesReturnReferenceTumorMode() throws Exception {
        Patient victim = reader(REFERENCE_AND_TUMOUR, PATIENT);
        assertThat(victim.reference()).isNotNull();
        assertThat(victim.tumor()).isNotNull();
    }

    @Test(expected = IllegalStateException.class)
    public void onlyOneSubdirectoryPresentInPatientDirectoryIfNoNameSpecified() throws Exception {
        reader(INFER_PATIENT_ID_AMBIGUOUS_DIRECTORIES, "");
    }

    @Test
    public void patientNameTakenFromSubdirectoryIfNoNameSpecified() throws Exception {
        Patient victim = reader(INFER_PATIENT_ID_PATIENT, "");
        assertThat(victim.name()).isEqualTo("CPCT12345678");
        assertThat(victim.reference()).isNotNull();
    }

    @Test
    public void recordGroupCorrectlyConstructedFromFileName() throws Exception {
        Patient victim = reader(SINGLE_SAMPLE, PATIENT);
        assertThat(victim.reference().lanes().get(0).recordGroupId()).isEqualTo("CPCT12345678_HJJLGCCXX_S17_L001_001");
    }

    @Test
    public void multipleFlowcellsWithSameLaneIndex() throws Exception {
        Patient victim = reader(MULTIPLE_FLOWCELLS, PATIENT);
        assertThat(victim.reference().lanes()).hasSize(2);
    }

    private static Patient reader(final String directory, final String patient) throws IOException {
        return PatientReader.fromHDFS(FileSystem.getLocal(new Configuration()), directory, patient);
    }
}