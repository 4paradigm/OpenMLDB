/*
 * Copyright 2022 paxos.cn.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.paxos.mysql.codec;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

public enum MysqlCharacterSet {
	BIG5_CHINESE_CI((byte) 1, "Big5"),
	LATIN2_CZECH_CS((byte) 2, "ISO8859_2"),
	DEC8_SWEDISH_CI((byte) 3, "ISO8859_1"),
	CP850_GENERAL_CI((byte) 4, "Cp437"),
	LATIN1_GERMAN1_CI((byte) 5, "ISO8859_1"),
	HP8_ENGLISH_CI((byte) 6, "ISO8859_1"),
	KOI8R_GENERAL_CI((byte) 7, "KOI8_R"),
	LATIN1_SWEDISH_CI((byte) 8, "ISO8859_1"),
	LATIN2_GENERAL_CI((byte) 9, "ISO8859_2"),
	SWE7_SWEDISH_CI((byte) 10, "ISO8859_1"),
	ASCII_GENERAL_CI((byte) 11, "US-ASCII"),
	UJIS_JAPANESE_CI((byte) 12, "EUC_JP"),
	SJIS_JAPANESE_CI((byte) 13, "SJIS"),
	CP1251_BULGARIAN_CI((byte) 14, "Cp1251"),
	LATIN1_DANISH_CI((byte) 15, "ISO8859_1"),
	HEBREW_GENERAL_CI((byte) 16, "HEBREW"),
	TIS620_THAI_CI((byte) 18, "TIS620"),
	EUCKR_KOREAN_CI((byte) 19, "EUCKR"),
	LATIN7_ESTONIAN_CS((byte) 20, "ISO8859_7"),
	LATIN2_HUNGARIAN_CI((byte) 21, "ISO8859_2"),
	KOI8U_GENERAL_CI((byte) 22, "KOI8_R"),
	CP1251_UKRAINIAN_CI((byte) 23, "Cp1251"),
	GB2312_CHINESE_CI((byte) 24, "GB2312"),
	GREEK_GENERAL_CI((byte) 25, "GREEK"),
	CP1250_GENERAL_CI((byte) 26, "Cp1250"),
	LATIN2_CROATIAN_CI((byte) 27, "ISO8859_2"),
	GBK_CHINESE_CI((byte) 28, "GBK"),
	CP1257_LITHUANIAN_CI((byte) 29, "Cp1257"),
	LATIN5_TURKISH_CI((byte) 30, "LATIN5"),
	LATIN1_GERMAN2_CI((byte) 31, "ISO8859_1"),
	ARMSCII8_GENERAL_CI((byte) 32, "ISO8859_1"),
	UTF8_GENERAL_CI((byte) 33, "UTF-8"),
	CP1250_CZECH_CS((byte) 34, "Cp1250"),
	UCS2_GENERAL_CI((byte) 35, "UTF-16BE"),
	CP866_GENERAL_CI((byte) 36, "Cp866"),
	KEYBCS2_GENERAL_CI((byte) 37, "Cp895"),
	MACCE_GENERAL_CI((byte) 38, "MacCentralEurope"),
	MACROMAN_GENERAL_CI((byte) 39, "MacRoman"),
	CP852_GENERAL_CI((byte) 40, "LATIN2"),
	LATIN7_GENERAL_CI((byte) 41, "ISO8859_7"),
	LATIN7_GENERAL_CS((byte) 42, "ISO8859_7"),
	MACCE_BIN((byte) 43, "MacCentralEurope"),
	CP1250_CROATIAN_CI((byte) 44, "Cp1250"),
	UTF8MB4_GENERAL_CI((byte) 45, "UTF-8"),
	LATIN1_BIN((byte) 47, "ISO8859_1"),
	LATIN1_GENERAL_CI((byte) 48, "ISO8859_1"),
	LATIN1_GENERAL_CS((byte) 49, "ISO8859_1"),
	CP1251_BIN((byte) 50, "Cp1251"),
	CP1251_GENERAL_CI((byte) 51, "Cp1251"),
	CP1251_GENERAL_CS((byte) 52, "Cp1251"),
	MACROMAN_BIN((byte) 53, "MacRoman"),
	CP1256_GENERAL_CI((byte) 57, "Cp1256"),
	CP1257_BIN((byte) 58, "Cp1257"),
	CP1257_GENERAL_CI((byte) 59, "Cp1257"),
	BINARY((byte) 63, "US-ASCII"),
	ARMSCII8_BIN((byte) 64, "ISO8859_2"),
	ASCII_BIN((byte) 65, "ASCII"),
	CP1250_BIN((byte) 66, "Cp1250"),
	CP1256_BIN((byte) 67, "Cp1256"),
	CP866_BIN((byte) 68, "Cp866"),
	DEC8_BIN((byte) 69, "US-ASCII"),
	GREEK_BIN((byte) 70, "GREEK"),
	HEBREW_BIN((byte) 71, "HEBREW"),
	HP8_BIN((byte) 72, "US-ASCII"),
	KEYBCS2_BIN((byte) 73, "Cp895"),
	KOI8R_BIN((byte) 74, "KOI8_R"),
	KOI8U_BIN((byte) 75, "KOI8_R"),
	LATIN2_BIN((byte) 77, "ISO8859_2"),
	LATIN5_BIN((byte) 78, "LATIN5"),
	LATIN7_BIN((byte) 79, "ISO8859_7"),
	CP850_BIN((byte) 80, "Cp437"),
	CP852_BIN((byte) 81, "Cp852"),
	SWE7_BIN((byte) 82, "ISO8859_1"),
	UTF8_BIN((byte) 83, "UTF-8"),
	BIG5_BIN((byte) 84, "Big5"),
	EUCKR_BIN((byte) 85, "EUCKR"),
	GB2312_BIN((byte) 86, "GB2312"),
	GBK_BIN((byte) 87, "GBK"),
	SJIS_BIN((byte) 88, "SJIS"),
	TIS620_BIN((byte) 89, "TIS620"),
	UCS2_BIN((byte) 90, "UTF-16BE"),
	UJIS_BIN((byte) 91, "EUC_JP"),
	GEOSTD8_GENERAL_CI((byte) 92, "US-ASCII"),
	GEOSTD8_BIN((byte) 93, "US-ASCII"),
	LATIN1_SPANISH_CI((byte) 94, "ISO8859_1"),
	CP932_JAPANESE_CI((byte) 95, "CP932"),
	CP932_BIN((byte) 96, "CP932"),
	EUCJPMS_JAPANESE_CI((byte) 97, "EUC_JP_Solaris"),
	EUCJPMS_BIN((byte) 98, "EUC_JP_Solaris"),
	UCS2_UNICODE_CI((byte) 128, "UTF-16BE"),
	UCS2_ICELANDIC_CI((byte) 129, "UTF-16BE"),
	UCS2_LATVIAN_CI((byte) 130, "UTF-16BE"),
	UCS2_ROMANIAN_CI((byte) 131, "UTF-16BE"),
	UCS2_SLOVENIAN_CI((byte) 132, "UTF-16BE"),
	UCS2_POLISH_CI((byte) 133, "UTF-16BE"),
	UCS2_ESTONIAN_CI((byte) 134, "UTF-16BE"),
	UCS2_SPANISH_CI((byte) 135, "UTF-16BE"),
	UCS2_SWEDISH_CI((byte) 136, "UTF-16BE"),
	UCS2_TURKISH_CI((byte) 137, "UTF-16BE"),
	UCS2_CZECH_CI((byte) 138, "UTF-16BE"),
	UCS2_DANISH_CI((byte) 139, "UTF-16BE"),
	UCS2_LITHUANIAN_CI((byte) 140, "UTF-16BE"),
	UCS2_SLOVAK_CI((byte) 141, "UTF-16BE"),
	UCS2_SPANISH2_CI((byte) 142, "UTF-16BE"),
	UCS2_ROMAN_CI((byte) 143, "UTF-16BE"),
	UCS2_PERSIAN_CI((byte) 144, "UTF-16BE"),
	UCS2_ESPERANTO_CI((byte) 145, "UTF-16BE"),
	UCS2_HUNGARIAN_CI((byte) 146, "UTF-16BE"),
	UTF8_UNICODE_CI((byte) 192, "UTF-8"),
	UTF8_ICELANDIC_CI((byte) 193, "UTF-8"),
	UTF8_LATVIAN_CI((byte) 194, "UTF-8"),
	UTF8_ROMANIAN_CI((byte) 195, "UTF-8"),
	UTF8_SLOVENIAN_CI((byte) 196, "UTF-8"),
	UTF8_POLISH_CI((byte) 197, "UTF-8"),
	UTF8_ESTONIAN_CI((byte) 198, "UTF-8"),
	UTF8_SPANISH_CI((byte) 199, "UTF-8"),
	UTF8_SWEDISH_CI((byte) 200, "UTF-8"),
	UTF8_TURKISH_CI((byte) 201, "UTF-8"),
	UTF8_CZECH_CI((byte) 202, "UTF-8"),
	UTF8_DANISH_CI((byte) 203, "UTF-8"),
	UTF8_LITHUANIAN_CI((byte) 204, "UTF-8"),
	UTF8_SLOVAK_CI((byte) 205, "UTF-8"),
	UTF8_SPANISH2_CI((byte) 206, "UTF-8"),
	UTF8_ROMAN_CI((byte) 207, "UTF-8"),
	UTF8_PERSIAN_CI((byte) 208, "UTF-8"),
	UTF8_ESPERANTO_CI((byte) 209, "UTF-8"),
	UTF8_HUNGARIAN_CI((byte) 210, "UTF-8"),
	UTF8MB4_UNICODE_CI((byte) 224, "UTF-8"),
	UTF8MB4_0900_AI_CI((byte) 255, "UTF-8");

	public static final MysqlCharacterSet DEFAULT = UTF8_GENERAL_CI;

	private byte id;
	private Charset charset;

	MysqlCharacterSet(byte id, String charsetName) {
		this.id = id;
		try {
			this.charset = Charset.forName(charsetName);
		} catch (UnsupportedCharsetException e) {
			this.charset = null;
		}
	}

	public static MysqlCharacterSet findById(int id) {
		for (MysqlCharacterSet charset : values()) {
			if (charset.id == id) {
				return charset;
			}
		}
		return null;
	}

	public Charset getCharset() {
		return charset;
	}

	public byte getId() {
		return id;
	}

	private static final AttributeKey<MysqlCharacterSet> SERVER_CHARSET_KEY = AttributeKey.newInstance(MysqlCharacterSet.class.getName() + "-server");
	private static final AttributeKey<MysqlCharacterSet> CLIENT_CHARSET_KEY = AttributeKey.newInstance(MysqlCharacterSet.class.getName() + "-client");

	public static MysqlCharacterSet getServerCharsetAttr(Channel channel) {
		return getCharSetAttr(SERVER_CHARSET_KEY, channel);
	}

	public static MysqlCharacterSet getClientCharsetAttr(Channel channel) {
		return getCharSetAttr(CLIENT_CHARSET_KEY, channel);
	}

	private static MysqlCharacterSet getCharSetAttr(AttributeKey<MysqlCharacterSet> key, Channel channel) {
		if (channel.hasAttr(key)) {
			return channel.attr(key).get();
		}
		return DEFAULT;
	}
}
