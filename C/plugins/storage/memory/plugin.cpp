#include <plugin_api.h>
#include <stdio.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"
#include <config_category.h>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <string>
#include <logger.h>
#include <reading_stream.h>
#include <mutex>
#include <thread>
#include <utility>

using namespace std;
using namespace rapidjson;

#define LEN_BUFFER_DATE 100
#define STORAGE_PURGE_SIZE	 0x0004U

/**
 * String escape routine
 */
const string escape(const string& str)
{
	char    *buffer;
	const char    *p1;
	char  *p2;
	string  newString;

	if (str.find_first_of('\'') == string::npos)
	{
		return str;
	}

	buffer = (char *)malloc(str.length() * 2);

	p1 = str.c_str();
	p2 = buffer;
	while (*p1)
	{
		if (*p1 == '\'')
		{
			*p2++ = '\'';
			*p2++ = '\'';
			p1++;
		}
		else
		{
			*p2++ = *p1++;
		}
	}
	*p2 = 0;
	newString = string(buffer);
	free(buffer);
	return newString;
}

bool formatDate(char *formatted_date, size_t buffer_size, const char *date) {

	struct timeval tv = {0};
	struct tm tm  = {0};
	char *valid_date = nullptr;

	// Extract up to seconds
	memset(&tm, 0, sizeof(tm));
	valid_date = strptime(date, "%Y-%m-%d %H:%M:%S", &tm);

	if (! valid_date)
	{
		return (false);
	}

	strftime (formatted_date, buffer_size, "%Y-%m-%d %H:%M:%S", &tm);

	// Work out the microseconds from the fractional part of the seconds
	char fractional[10] = {0};
	sscanf(date, "%*d-%*d-%*d %*d:%*d:%*d.%[0-9]*", fractional);
	// Truncate to max 6 digits
	fractional[6] = 0;
	int multiplier = 6 - (int)strlen(fractional);
	if (multiplier < 0)
		multiplier = 0;
	while (multiplier--)
		strcat(fractional, "0");

	strcat(formatted_date ,".");
	strcat(formatted_date ,fractional);

	// Handles timezone
	char timezone_hour[5] = {0};
	char timezone_min[5] = {0};
	char sign[2] = {0};

	sscanf(date, "%*d-%*d-%*d %*d:%*d:%*d.%*d-%2[0-9]:%2[0-9]", timezone_hour, timezone_min);
	if (timezone_hour[0] != 0)
	{
		strcat(sign, "-");
	}
	else
	{
		memset(timezone_hour, 0, sizeof(timezone_hour));
		memset(timezone_min,  0, sizeof(timezone_min));

		sscanf(date, "%*d-%*d-%*d %*d:%*d:%*d.%*d+%2[0-9]:%2[0-9]", timezone_hour, timezone_min);
		if  (timezone_hour[0] != 0)
		{
			strcat(sign, "+");
		}
		else
		{
			// No timezone is expressed in the source date
			// the default UTC is added
			strcat(formatted_date, "+00:00");
		}
	}

	if (sign[0] != 0)
	{
		if (timezone_hour[0] != 0)
		{
			strcat(formatted_date, sign);

			// Pad with 0 if an hour having only 1 digit was provided
			// +1 -> +01
			if (strlen(timezone_hour) == 1)
				strcat(formatted_date, "0");

			strcat(formatted_date, timezone_hour);
			strcat(formatted_date, ":");
		}

		if (timezone_min[0] != 0)
		{
			strcat(formatted_date, timezone_min);

			// Pad with 0 if minutes having only 1 digit were provided
			// 3 -> 30
			if (strlen(timezone_min) == 1)
				strcat(formatted_date, "0");

		}
		else
		{
			// Minutes aren't expressed in the source date
			strcat(formatted_date, "00");
		}
	}

	return true;
}

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

struct Reading {
	string _assetCode;
	string _userTs;
	string _ts;
	Value _json;

	Reading(string assetCode, string userTs, string ts, Value json)
		: _assetCode(std::move(assetCode)),
		  _userTs(std::move(userTs)),
		  _ts(std::move(ts)),
		  _json(std::move(json)) {
	}
};

class MemoryContext {
public:
	MemoryContext()
		: _readingMinId(0) {
	}

	void addReading(const string& assetCode, const string& userTs, Value json);
	void purgeReadingsByRow(unsigned long maxRows, unsigned long sent, unsigned long& removed, unsigned long& unsentPurged, unsigned long& unsentRetained,
							unsigned long& readings, unsigned int& duration);
	void purgeReadingsByAge(unsigned long maxAge, unsigned long sent, unsigned long& removed, unsigned long& unsentPurged, unsigned long& unsentRetained,
						    unsigned long& readings, unsigned int& duration);
	Document fetchReadings(unsigned long firstId, unsigned int blkSize);

private:
	std::mutex  _mutex;
	unsigned long _readingMinId;
	vector<Reading> _readings;
};

void MemoryContext::addReading(const string& assetCode, const string& userTs, Value json) {
	// add current date time with micro seconds
	auto now = std::chrono::system_clock::now();
	auto epoch = now.time_since_epoch();
	auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(epoch) % 1000000;
	auto time = std::chrono::system_clock::to_time_t(now);
	std::stringstream ts;
	std::tm tm = *std::gmtime(&time);
	ts << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << "." << std::setfill('0') << std::setw(6) << microseconds.count();
	// add time zone
	char formattedDate[LEN_BUFFER_DATE] = {};
	formatDate(formattedDate, sizeof(formattedDate), ts.str().c_str());
	std::lock_guard<std::mutex> lock(_mutex);
	_readings.emplace_back(assetCode, userTs, formattedDate, std::move(json));
}

void MemoryContext::purgeReadingsByRow(unsigned long maxRows, unsigned long sent, unsigned long& removed, unsigned long& unsentPurged, unsigned long& unsentRetained,
									   unsigned long& readings, unsigned int& duration) {
	std::lock_guard<std::mutex> lock(_mutex);
	if (_readings.size() > maxRows) {
		removed = _readings.size() - maxRows;
		_readings.erase(_readings.begin(), _readings.begin() + removed);
		_readingMinId += removed;
		readings = _readings.size();
		// TODO duration to current value?
		Logger::getLogger()->debug("%d reading have been purged by row, %d remains", removed, _readings.size());
	}
}

void MemoryContext::purgeReadingsByAge(unsigned long maxAge, unsigned long sent, unsigned long& removed, unsigned long& unsentPurged, unsigned long& unsentRetained,
								       unsigned long& readings, unsigned int& duration) {
	std::lock_guard<std::mutex> lock(_mutex);
	duration = maxAge;
	readings = _readings.size();
	Logger::getLogger()->debug("purged by date not impl, %d remains", _readings.size());
}


// iDs seems to start at 1
Document MemoryContext::fetchReadings(unsigned long firstId, unsigned int blkSize) {
	Document doc;
	doc.SetObject();
	Document::AllocatorType& allocator = doc.GetAllocator();

	Value rows(kArrayType);

	unsigned long fetch_count = 0;
	std::lock_guard<std::mutex> lock(_mutex);
	if (firstId >= _readingMinId + 1 && firstId <= _readingMinId + _readings.size()) {
		unsigned long windowFirstId = firstId - _readingMinId - 1;
		unsigned long windowSize = std::min(_readings.size() - windowFirstId, (unsigned long) blkSize);

		// Logger::getLogger()->debug("MEMORY plugin_reading_fetch firstId= %d, blksize=%d, readingsCount=%d, windowFirstId=%d, windowSize=%d",
		// 	firstId, blkSize, _readings.size(), windowFirstId, windowSize);

		fetch_count = windowSize;

		for (unsigned long i = windowFirstId; i < windowFirstId + windowSize; i++) {
			Reading& reading = _readings[i];

			Value row(kObjectType);

			unsigned long id = firstId + i - windowFirstId;
			row.AddMember("id", id, allocator);

			// Logger::getLogger()->debug("MEMORY plugin_reading_fetch id=%d, assetCode='%s', userTs='%s', ts='%s'",
			// 	id, reading._assetCode.c_str(), reading._userTs.c_str(), reading._ts.c_str());

			Value assetCodeValue(reading._assetCode.c_str(), allocator); // TODO No need to copy the string?
			row.AddMember("asset_code", assetCodeValue, allocator);

			Value userTsValue(reading._userTs.c_str(), allocator); // TODO No need to copy the string?
			row.AddMember("user_ts", userTsValue, allocator);

			Value tsValue(reading._ts.c_str(), allocator); // TODO No need to copy the string?
			row.AddMember("ts", tsValue, allocator);

			// create a copy of the json model
			Value copiedJson(kObjectType);
			copiedJson.CopyFrom(reading._json, allocator);
			row.AddMember("reading", copiedJson, allocator);

			rows.PushBack(row, allocator);
		}
	}

	Value count;
	count.SetUint64(fetch_count);
	if (fetch_count > 0) {
		Logger::getLogger()->debug("%d readings has been fetched", fetch_count);
	}

	doc.AddMember("count", count, allocator)
		.AddMember("rows", rows, allocator);

	return doc;
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

	Document doc;
	ParseResult ok = doc.Parse(readings);
	if (!ok)
	{
		Logger::getLogger()->error("Fail to parse document: %s", GetParseError_En(doc.GetParseError()));
		return -1;
	}

	if (!doc.HasMember("readings"))
	{
		Logger::getLogger()->error("Payload is missing a readings array");
		return -1;
	}

	Value &readingsValue = doc["readings"];
	if (!readingsValue.IsArray())
	{
		Logger::getLogger()->error("Payload is missing the readings array");
		return -1;
	}

	int updateAssetCount = 0;
	for (Value::ValueIterator itr = readingsValue.Begin(); itr != readingsValue.End(); ++itr)
	{
		if (!itr->IsObject())
		{
			Logger::getLogger()->error("Each reading in the readings array must be an object");
			return -1;
		}

		Value* readingValuePtr = itr;
		Value& readingValue = *readingValuePtr;

		// Handles - asset_code
		const auto assetCode = readingValue["asset_code"].GetString();

		if (strlen(assetCode) == 0)
		{
			Logger::getLogger()->warn("Empty asset code value, row ignored.");
			continue;
		}

		// Handles - user_ts
		char formatted_date[LEN_BUFFER_DATE] = {0};
		const char* user_ts = (*itr)["user_ts"].GetString();
		if (!formatDate(formatted_date, sizeof(formatted_date), user_ts) )
		{
			Logger::getLogger()->error("Invalid date |%s|", user_ts);
			return -1;
		}

		Value& readingData = readingValue["reading"];

		// detach the value from the original document
		Value detachedValue;
		detachedValue.CopyFrom(readingValue, doc.GetAllocator());
		readingData.SetNull();

		context->addReading(assetCode, user_ts, std::move(readingData));
		updateAssetCount++;
	}
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

	Document doc = context->fetchReadings(id, blksize);

	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	doc.Accept(writer);
	char* json = (char *) buffer.GetString();

//	Logger::getLogger()->debug("MEMORY plugin_reading_fetch json %s", json);
	return strdup(json);
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
