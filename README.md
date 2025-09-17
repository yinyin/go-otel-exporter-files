# File path structure

The file path will be: `${GivenBaseFolderPath}/${Base32-Hour}/${Base32-SerialNumber}`

Where:

- Base32 is encoded with `0123456789abcdefghijklmnopqrstuv` as encoder template without paddings.
- Hour is hours from UNIX epoch with byte mask `0xFFFFFF` applied. Next overflow would occurs at 3883-12-08 15:59:59 UTC.
- SerialNumber starts from `0`.
  + Value smaller or equal to `0xFFFF` (65536), the value will encode with 2 bytes.
  + Value larger than `0xFFFF`, the value will encode with 4 bytes.
  + The value stops incremention at `0x7FFFFFFD`. Records will be throw away. This situation should be avoid.

# File rotation

The records will be save in the same file if:

- Same Hour value.
- Empty file (written size is `0`) or smaller than limitation after the record is written.
- File is opened for write.

The records will be save in the same folder if:

- Same Hour value.

# Purging

The purge check will conduct when new folder is opened. Only 2 expired hours will be checked for purging.