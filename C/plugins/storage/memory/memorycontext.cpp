#include <plugin_api.h>
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
#include <memorycontext.h>

#define LEN_BUFFER_DATE 100

using namespace std;
using namespace rapidjson;

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

int MemoryContext::addReading(const char* readings) {
	// get current date time with micro seconds
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

	Document doc;
	doc.Parse(readings);
	Value &readingsValue = doc["readings"];
	int updateAssetCount = 0;
	for (Value::ValueIterator itr = readingsValue.Begin(); itr != readingsValue.End(); ++itr) {
		Value* readingValuePtr = itr;
		Value& readingValue = *readingValuePtr;

		const auto assetCode = readingValue["asset_code"].GetString();

		char formatted_date[LEN_BUFFER_DATE] = {};
		const char* userTs = (*itr)["user_ts"].GetString();
		formatDate(formatted_date, sizeof(formatted_date), userTs);

		Value& readingData = readingValue["reading"];
		// detach the value from the original document
		readingData.SetNull();

		rwLock.lockWrite();
		_readings.emplace_back(assetCode, userTs, formattedDate, std::move(readingData));
		rwLock.unlockWrite();
		updateAssetCount++;
	}

	return updateAssetCount;
}

void MemoryContext::purgeReadingsByRow(unsigned long maxRows, unsigned long sent, unsigned long& removed, unsigned long& unsentPurged, unsigned long& unsentRetained,
									   unsigned long& readings, unsigned int& duration) {
	rwLock.lockWrite();
	if (_readings.size() > maxRows) {
		removed = _readings.size() - maxRows;
		_readings.erase(_readings.begin(), _readings.begin() + removed);
		_readingMinId += removed;
		readings = _readings.size();
		// TODO duration to current value?
		Logger::getLogger()->debug("%d reading have been purged by row, %d remains", removed, _readings.size());
	}
	rwLock.unlockWrite();
}

void MemoryContext::purgeReadingsByAge(unsigned long maxAge, unsigned long sent, unsigned long& removed, unsigned long& unsentPurged, unsigned long& unsentRetained,
								       unsigned long& readings, unsigned int& duration) {
	rwLock.lockWrite();
	duration = maxAge;
	readings = _readings.size();
	Logger::getLogger()->debug("purged by date not impl, %d remains", _readings.size());
	rwLock.unlockWrite();
}


// iDs seems to start at 1
Document MemoryContext::fetchReadings(unsigned long firstId, unsigned int blkSize) {
	Document doc;
	doc.SetObject();
	Document::AllocatorType& allocator = doc.GetAllocator();

	Value rows(kArrayType);

	unsigned long fetch_count = 0;
	rwLock.lockRead();
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

			Value assetCodeValue(rapidjson::kStringType);
			assetCodeValue.SetString(reading._assetCode.c_str(), static_cast<rapidjson::SizeType>(reading._assetCode.length()), allocator);
			row.AddMember("asset_code", assetCodeValue, allocator);

			Value userTsValue(rapidjson::kStringType);
			userTsValue.SetString(reading._userTs.c_str(), static_cast<rapidjson::SizeType>(reading._userTs.length()), allocator);
			row.AddMember("user_ts", userTsValue, allocator);

			Value tsValue(rapidjson::kStringType);
			tsValue.SetString(reading._ts.c_str(), static_cast<rapidjson::SizeType>(reading._ts.length()), allocator);
			row.AddMember("ts", tsValue, allocator);

			row.AddMember("reading", reading._json, allocator);

			rows.PushBack(row, allocator);
		}
	}
	rwLock.unlockRead();

	Value count;
	count.SetUint64(fetch_count);
	if (fetch_count > 0) {
		Logger::getLogger()->debug("%d readings has been fetched", fetch_count);
	}

	doc.AddMember("count", count, allocator)
		.AddMember("rows", rows, allocator);

	return doc;
}
