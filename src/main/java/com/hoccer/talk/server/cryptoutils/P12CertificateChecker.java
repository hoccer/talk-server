package com.hoccer.talk.server.cryptoutils;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;

public class P12CertificateChecker {

    private final String mP12CertificatePath;
    private final String mImportPassword;
    private Date expiryDate;

    public P12CertificateChecker(String certificatePath, String importPassword) {
        mP12CertificatePath = certificatePath;
        mImportPassword = importPassword;
        verifyCertificatePath();
    }

    public Date getCertificateExpiryDate() throws IOException {
        if (expiryDate != null) {
            return expiryDate;
        }
        // first convert the p12 into a pem...
        // openssl pkcs12 -in <certificateFilePath> -clcerts -nokeys -out temp.pem
        Path tempDir = Files.createTempDirectory("p12check");
        String tempPemPath = tempDir + "/temp.pem";
        executeCommand("openssl pkcs12 -in " + mP12CertificatePath + " -clcerts -nokeys -out " + tempPemPath + " -passin pass:\"" + mImportPassword + "\"");

        // Determime expiry date of the extracted pem
        // openssl x509 -in temp.pem -noout -enddate
        String rawResult = executeCommand("openssl x509 -in " + tempPemPath + " -noout -enddate").trim();
        String[] segments = rawResult.split("=");
        expiryDate = new Date(segments[1]); // Date parsing in Java sucks
        return expiryDate;
    }

    public boolean isExpired() throws IOException {
        if (expiryDate == null) {
            getCertificateExpiryDate();
        }
        return new Date().after(expiryDate);
    }

    private void verifyCertificatePath() throws IllegalArgumentException {
        File p12cert = new File(mP12CertificatePath);
        if (!p12cert.exists()) {
            throw new IllegalArgumentException("P12 Certificate does not exist: '" + mP12CertificatePath + "'");
        }
    }

    private String executeCommand(String command) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DefaultExecutor executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(outputStream));
        executor.execute(CommandLine.parse(command));
        return outputStream.toString();
    }
}