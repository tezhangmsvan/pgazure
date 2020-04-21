#include <was/storage_account.h>
#include <was/blob.h>
#include <cpprest/filestream.h>
#include <cpprest/containerstream.h>
#include <cpprest/interopstream.h>

#include "pgazure/cpp_utils.h"
#include "pgazure/blob_storage.h"


void
CheckForInterrupts(void)
{
	if (IsQueryCancelPending())
	{
		throw std::runtime_error("canceling statement due to user request");
	}
}


extern "C" int ReadFromStdInputStream(void *context, void *buf, int minRead, int maxRead);
extern "C" void CloseStdInputStream(void *context);

int
ReadFromStdInputStream(void *context, void *outBuf, int minRead, int maxRead)
{
	std::istream *syncStream = (std::istream *) context;
	char *buf = (char *) outBuf;
	int bytesRead = 0;

	do
	{
		if (!syncStream->read(buf + bytesRead, maxRead - bytesRead))
		{
			/* reached the end of stream, but may still have gotten some bytes */
			bytesRead += syncStream->gcount();
			break;
		}

		bytesRead += syncStream->gcount();
	}
	while (bytesRead < minRead);

	return bytesRead;
}

void
CloseStdInputStream(void *context)
{
	std::istream *syncStream = (std::istream *) context;
	delete syncStream;
}



void
ReadBlockBlob(char *connectionString, char *containerName, char *path, ByteSource *byteSource)
{
	try
	{
		azure::storage::cloud_storage_account storage_account = azure::storage::cloud_storage_account::parse(connectionString);
		azure::storage::cloud_blob_client blob_client = storage_account.create_cloud_blob_client();
		azure::storage::cloud_blob_container container = blob_client.get_container_reference(U(containerName));
		   
		azure::storage::cloud_block_blob block_blob = container.get_block_blob_reference(U(path));
		concurrency::streams::istream blockStream = block_blob.open_read();
		Concurrency::streams::async_istream<char> *syncStream = new Concurrency::streams::async_istream<char>(blockStream);

		byteSource->context = (void *) syncStream;
		byteSource->read = ReadFromStdInputStream;
		byteSource->close = CloseStdInputStream;
	}
	catch (const azure::storage::storage_exception& e)
	{
		ucout << U("Error: ") << e.what() << std::endl;

		azure::storage::request_result result = e.result();
		azure::storage::storage_extended_error extended_error = result.extended_error();
		if (!extended_error.message().empty())
		{
			ThrowPostgresError(extended_error.message().c_str());
		}
		else
		{
			ThrowPostgresError(e.what());
		}
	}
	catch (const std::exception& e)
	{
		ThrowPostgresError(e.what());
	}
}

class BlockBlobWriter {
		azure::storage::cloud_storage_account storage_account;
		azure::storage::cloud_blob_client blob_client;
		azure::storage::cloud_blob_container container;
		azure::storage::cloud_block_blob block_blob;
		concurrency::streams::ostream blockStream;
		concurrency::streams::streambuf<char> streamBuf;
		//Concurrency::streams::async_ostream<char> *syncStream;

	public:
		BlockBlobWriter(char *connectionString, char *containerName, char *path);
		void write(const char *buf, int bytesToWrite);
		void close();

};

BlockBlobWriter::BlockBlobWriter(char *connectionString, char *containerName, char *path)
{
	storage_account = azure::storage::cloud_storage_account::parse(connectionString);
	blob_client = storage_account.create_cloud_blob_client();
	container = blob_client.get_container_reference(U(containerName));
	block_blob = container.get_block_blob_reference(U(path));
	blockStream = block_blob.open_write();
	streamBuf = blockStream.streambuf();
	//syncStream = new Concurrency::streams::async_ostream<char>(blockStream);
}

void BlockBlobWriter::write(const char *buf, int bytesToWrite)
{
	try
	{
		std::vector<uint8_t> buffer(buf, buf + bytesToWrite);
		concurrency::streams::container_buffer<std::vector<uint8_t>> sbuf(buffer);
		blockStream.write(sbuf, bytesToWrite).wait();
/*
		blockStream.streambuf().putn_nocopy(buf, bytesToWrite).wait();
*/
		//syncStream->write(buf, bytesToWrite);
	}
	catch (const std::exception& e)
	{
		ThrowPostgresError(e.what());
	}
}

void BlockBlobWriter::close()
{
	blockStream.flush().wait();
	blockStream.close().wait();
}


int
WriteToBlockBlobWriter(void *context, void *buf, int bytesToWrite)
{
	BlockBlobWriter *writer = (BlockBlobWriter *) context;

	writer->write((const char *) buf, bytesToWrite);
	return 0;
}

void
CloseBlockBlobWriter(void *context)
{
	BlockBlobWriter *writer = (BlockBlobWriter *) context;
	writer->close();
	delete writer;
}



void
WriteBlockBlob(char *connectionString, char *containerName, char *path, ByteSink *byteSink)
{
	try
	{
		BlockBlobWriter *writer = new BlockBlobWriter(connectionString, containerName, path);

		byteSink->context = (void *) writer;
		byteSink->write = WriteToBlockBlobWriter;
		byteSink->close = CloseBlockBlobWriter;
	}
	catch (const azure::storage::storage_exception& e)
	{
		ucout << U("Error: ") << e.what() << std::endl;

		azure::storage::request_result result = e.result();
		azure::storage::storage_extended_error extended_error = result.extended_error();
		if (!extended_error.message().empty())
		{
			ThrowPostgresError(extended_error.message().c_str());
		}
		else
		{
			ThrowPostgresError(e.what());
		}
	}
	catch (const std::exception& e)
	{
		ThrowPostgresError(e.what());
	}
}




std::vector<azure::storage::cloud_blob>
ListEntireBlobTree(const azure::storage::cloud_blob_container& container, std::string prefix, int maxResultCount, const azure::storage::blob_request_options& options)
{
    std::vector<azure::storage::cloud_blob> blobs;
    azure::storage::continuation_token token;
	azure::storage::operation_context context;

    do
    {
        auto results = container.list_blobs_segmented(prefix, true, azure::storage::blob_listing_details::metadata, maxResultCount, token, options, context);

        for (auto& item : results.results())
        {
            if (item.is_blob())
            {
                blobs.push_back(std::move(item.as_blob()));
            }
        }

		CheckForInterrupts();

        token = results.continuation_token();
    } while (!token.empty());

    return blobs;
}


void
ListBlobs(char *connectionString, char *containerName, char *prefix, void (*processBlob)(void *, CloudBlob *), void *processBlobContext)
{
	try
	{
		azure::storage::cloud_storage_account storage_account = azure::storage::cloud_storage_account::parse(connectionString);
		azure::storage::cloud_blob_client blob_client = storage_account.create_cloud_blob_client();
		azure::storage::cloud_blob_container container = blob_client.get_container_reference(U(containerName));
		std::string prefixString(prefix);

		auto results = ListEntireBlobTree(container, prefixString, 1000, azure::storage::blob_request_options());

		for(azure::storage::cloud_blob blob : results)
		{
			azure::storage::cloud_blob_properties properties = blob.properties();

			/* keep strings on the stack */
			std::string lastModifiedString = properties.last_modified().to_string();

			CloudBlob cloudBlob;
			memset(&cloudBlob, 0, sizeof(cloudBlob));

			cloudBlob.name = blob.name().c_str();
			cloudBlob.size = properties.size();
			cloudBlob.contentEncoding = properties.content_encoding().c_str();
			cloudBlob.contentLanguage = properties.content_language().c_str();
			cloudBlob.contentMD5 = properties.content_md5().c_str();
			cloudBlob.contentType = properties.content_type().c_str();
			cloudBlob.etag = properties.etag().c_str();
			cloudBlob.lastModified = lastModifiedString.c_str();

			processBlob(processBlobContext, &cloudBlob);
		}
	}
	catch (const std::exception& e)
	{
		ThrowPostgresError(e.what());
	}
}
