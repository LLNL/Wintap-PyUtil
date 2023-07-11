/*
 * Macros used as UDFs
 *
 */

CREATE OR REPLACE macro win32_to_epoch(wts) AS wts/1e7 - 11644473600;

CREATE OR REPLACE macro int_to_ip(i) AS concat_ws('.',i >> 24,i >> 16 & 255,i >> 8 & 255,i & 255);

CREATE OR REPLACE macro to_timestamp_micros(es)
AS to_timestamp(cast(floor(es) AS bigint)) + to_microseconds(cast(floor((es - floor(es)) * 1e6) as bigint));
