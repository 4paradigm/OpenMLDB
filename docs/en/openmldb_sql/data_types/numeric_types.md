# Numeric Type

OpenMLDB boolean types supported:

| type | size   | range（signed） | range（unsigned） | use     |
| :--- | :----- | :------------- | :------------- | :------- |
| BOOL | 1 byte | (-128，127)    | (0，255)       | small integer value |

## Integer Type (Exact Value)

OpenMLDB supports integer types: `INT`, `SMALLINT`, `BIGINT`

| type            | size    | range (signed）                                          | range (unsigned）                  | use       |
| :-------------- | :------ | :------------------------------------------------------ | :------------------------------ | :--------- |
| SMALLINT or INT16 | 2 bytes | (-32 768，32 767)                                       | (0，65 535)                     | small integer value   |
| INT or INT32      | 4 bytes | (-2 147 483 648，2 147 483 647)                         | (0，4 294 967 295)              | large integer value   |
| BIGINT or INT64   | 8 bytes | (-9,223,372,036,854,775,808，9 223 372 036 854 775 807) | (0，18 446 744 073 709 551 615) | very large integer value |

## Floating Point Type (Approximate Value)

OpenMLDB supports two floating point types: `FLOAT`, `DOUBLE`

| type   | size    | range（signed）                                               | range（unsigned）                                               | use            |
| :----- | :------ | :----------------------------------------------------------- | :----------------------------------------------------------- | :-------------- |
| FLOAT  | 4 bytes | (-3.402 823 466 E+38，-1.175 494 351 E-38)，0，(1.175 494 351 E-38，3.402 823 466 351 E+38) | 0，(1.175 494 351 E-38，3.402 823 466 E+38)                  | single precision floating point value |
| DOUBLE | 8 bytes | (-1.797 693 134 862 315 7 E+308，-2.225 073 858 507 201 4 E-308)，0，(2.225 073 858 507 201 4 E-308，1.797 693 134 862 315 7 E+308) | 0，(2.225 073 858 507 201 4 E-308，1.797 693 134 862 315 7 E+308) | double-precision floating-point value |
