import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.ListPathsOptions;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;

public class AzureBlobStorageSample1 {

  public static void main(String[] args) {
    AzureBlobStorageSample1 sample1 = new AzureBlobStorageSample1();
    sample1.runListAllFiles("parquet");
  }

  private void runListAllFiles(String fsName) {
    Duration timeoutSec = Duration.ofSeconds(60);


    //    String endPoint =
    //        String.format(Locale.ROOT, "https://%s.blob.core.windows.net/%s", account, "parquet");

    String endPoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", getAccount());

    DataLakeServiceClient serviceClient =
        new DataLakeServiceClientBuilder()
            .endpoint(endPoint)
            .credential(getSharedKeyCredential())
            .buildClient();

    DataLakeFileSystemClient fileSystemClient = serviceClient.getFileSystemClient(fsName);

    ListPathsOptions lpo = new ListPathsOptions();
    lpo.setPath("/");
    lpo.setRecursive(true);

    fileSystemClient
        .listPaths(lpo, null)
        .forEach(
            fsi -> {
              System.out.println(
                  "ADLSv2 Item: "
                      + fsi.getName()
                      + " - Type: "
                      + (fsi.isDirectory() ? "Directory" : "File"));
            });
  }

  private void runListAllFilesystems() {
    Duration timeoutSec = Duration.ofSeconds(60);

    String account = "knowbistorage";
    String endPoint =
        String.format(Locale.ROOT, "https://%s.blob.core.windows.net/%s", account, "parquet");

    DataLakeServiceClient serviceClient =
        new DataLakeServiceClientBuilder()
            .endpoint(endPoint)
            .credential(getSharedKeyCredential())
            .buildClient();

    serviceClient
        .listFileSystems()
        .forEach(
            s -> {
              System.out.println("Filesystem name: " + s.getName());
            });
  }

  private void runSampleBlob() {

    Duration timeoutSec = Duration.ofSeconds(60);
    String account = "knowbistorage";

    String endPoint =
        String.format(Locale.ROOT, "https://%s.blob.core.windows.net/%s", account, "parquet");

    // Create a BlobServiceClient object using a connection string
    BlobContainerClient blobContainerClient =
        new BlobContainerClientBuilder()
            .endpoint(endPoint)
            .credential(getSharedKeyCredential())
            .buildClient();

    // BlobServiceClient serviceClient = blobContainerClient.getServiceClient();

    blobContainerClient
        .listBlobsByHierarchy("/output/books2.csv")
        .forEach(
            blobItem -> {
              BlobClient client = blobContainerClient.getBlobClient(blobItem.getName());
              String blobContainerUrl = blobContainerClient.getBlobContainerUrl();
              String blobUri =
                  URLDecoder.decode(client.getBlobUrl())
                      .substring(
                          blobContainerUrl.length() + 1,
                          URLDecoder.decode(client.getBlobUrl()).length());
              BlobItemProperties blobItemProperties = blobItem.getProperties();
              System.out.println(
                  "File: "
                      + blobItem.getName()
                      + " - Prefix: "
                      + blobItem.isPrefix()
                      //                      + (blobItemProperties != null &&
                      // blobItemProperties.getContentDisposition() != null
                      //                          ? " - Content Disposition: " +
                      // blobItemProperties.getContentDisposition()
                      //                          : "")
                      //                      + " - Blob Container Url: "
                      //                      + blobContainerUrl
                      + " - Uri: "
                      + blobUri);
            });
  }

  protected StorageSharedKeyCredential getSharedKeyCredential() {
    Properties prop = new Properties();
    try (InputStream input =
        AzureBlobStorageSample1.class.getClassLoader().getResourceAsStream("azure.properties")) {
      prop.load(input);
      String account = prop.getProperty("account");
      String key = prop.getProperty("key");

      StorageSharedKeyCredential storageCreds = new StorageSharedKeyCredential(account, key);
      return storageCreds;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected String getAccount() {
    Properties prop = new Properties();
    try (InputStream input =
        AzureBlobStorageSample1.class.getClassLoader().getResourceAsStream("azure.properties")) {
      prop.load(input);
      String account = prop.getProperty("account");
      return account;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
