#include <plugin_api.h>
#include <config_category.h>
#include <string>
#include <logger.h>
#include <reading_stream.h>
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include <memorycontext.h>

#define STORAGE_PURGE_SIZE	 0x0004U

using namespace std;
using namespace rapidjson;

/**
 * The memory plugin interface
 */
extern "C" {

const char *default_config = QUOTE({
});

/**
 * The plugin information structure
 */
static PLUGIN_INFORMATION info = {
	"Memory",		// Name
	"1.0.0",		// Version
	SP_READINGS,		// Flags
	PLUGIN_TYPE_STORAGE,	// Type
	"1.6.0",		// Interface version
	default_config
};

/**
 * Return the information about this plugin
 */
PLUGIN_INFORMATION *plugin_info()
{
	return &info;
}

/**
 * Initialise the plugin, called to get the plugin handle
 * In the case of SQLLite we also get a pool of connections
 * to use.
 *
 * @param category	The plugin configuration category
 */
PLUGIN_HANDLE plugin_init(ConfigCategory *category)
{
	Logger::getLogger()->debug("MEMORY plugin_init");
	return new MemoryContext();
}

/**
 * Append a sequence of readings to the readings buffer
 */
int plugin_reading_append(PLUGIN_HANDLE handle, char *readings)
{
//	Logger::getLogger()->debug("MEMORY plugin_reading_append '%s'", readings);

	auto context = static_cast<MemoryContext *>(handle);

	int updateAssetCount = context->addReading(readings);

	Logger::getLogger()->info("%d assets have been updated", updateAssetCount);

	return 0;
}

/**
 * Append a stream of readings to the readings buffer
 */
int plugin_readingStream(PLUGIN_HANDLE handle, ReadingStream **readings, bool commit)
{
	Logger::getLogger()->debug("MEMORY plugin_readingStream");
	// TODO
	return 0;
}

/**
 * Fetch a block of readings from the readings buffer
 */
char *plugin_reading_fetch(PLUGIN_HANDLE handle, unsigned long id, unsigned int blksize) {
//	Logger::getLogger()->debug("MEMORY plugin_reading_fetch %d %d", id, blksize);

	auto context = static_cast<MemoryContext *>(handle);

	string json = context->fetchReadings(id, blksize);

//	Logger::getLogger()->debug("MEMORY plugin_reading_fetch json %s", json.c_str());

	return strdup(json.c_str());
}

/**
 * Retrieve some readings from the readings buffer
 */
char *plugin_reading_retrieve(PLUGIN_HANDLE handle, char *condition)
{
	Logger::getLogger()->debug("MEMORY plugin_reading_retrieve '%s'", condition);
	// TODO only called at startup so OK to return 0 ?
	return strdup("{\"count\":0,\"rows\":[]}");
}

/**
 * Purge readings from the buffer
 */
char *plugin_reading_purge(PLUGIN_HANDLE handle, unsigned long param, unsigned int flags, unsigned long sent)
{
	Logger::getLogger()->debug("MEMORY plugin_reading_purge");

	auto context = static_cast<MemoryContext *>(handle);

	string method;
	unsigned long removed = 0;
	unsigned long unsentPurged = 0;
	unsigned long unsentRetained = 0;
	unsigned long readings = 0;
	unsigned int duration = 0;
	if (flags & STORAGE_PURGE_SIZE) { // Purge by size
		method = "row";
		context->purgeReadingsByRow(param, sent, removed, unsentPurged, unsentRetained, readings, duration);
	}
	else {
		method = "age";
		context->purgeReadingsByAge(param, removed, sent, unsentPurged, unsentRetained, readings, duration);
		// TODO
	}

	string result = "{ \"removed\" : " + std::to_string(removed) + ", ";
	result += " \"unsentPurged\" : " + std::to_string(unsentPurged) + ", ";
	result += " \"unsentRetained\" : " + std::to_string(unsentRetained) + ", ";
	result += " \"readings\" : " + std::to_string(readings) + ", ";
	result += " \"method\" : \"" + method + "\", ";
	result += " \"duration\" : " + std::to_string(duration) + " }";

	Logger::getLogger()->debug("MEMORY plugin_reading_purge result %s", result.c_str());

	return strdup(result.c_str());
}

/**
 * Release a previously returned result set
 */
void plugin_release(PLUGIN_HANDLE handle, char *results)
{
	Logger::getLogger()->debug("MEMORY plugin_release");
	// TODO
}

/**
 * Return details on the last error that occured.
 */
PLUGIN_ERROR *plugin_last_error(PLUGIN_HANDLE handle)
{
	Logger::getLogger()->debug("MEMORY plugin_last_error");
	// TODO
	return nullptr;
}

/**
 * Shutdown the plugin
 */
bool plugin_shutdown(PLUGIN_HANDLE handle)
{
	Logger::getLogger()->debug("MEMORY plugin_shutdown");
	delete static_cast<MemoryContext*>(handle);
	return true;
}

/**
 * Purge given readings asset or all readings from the buffer
 */
unsigned int plugin_reading_purge_asset(PLUGIN_HANDLE handle, char *asset)
{
	Logger::getLogger()->debug("MEMORY plugin_reading_purge_asset");
	return 0;
}

}
