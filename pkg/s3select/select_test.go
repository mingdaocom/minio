/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package s3select

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"testing"
)

type testResponseWriter struct {
	statusCode int
	response   []byte
}

func (w *testResponseWriter) Header() http.Header {
	return nil
}

func (w *testResponseWriter) Write(p []byte) (int, error) {
	w.response = append(w.response, p...)
	return len(p), nil
}

func (w *testResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}

func (w *testResponseWriter) Flush() {
}

func TestCSVInput(t *testing.T) {
	var testTable = []struct {
		requestXML     []byte
		expectedResult []byte
	}{
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT one, two, three from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0, 0, 0, 137, 0, 0, 0, 85, 194, 213, 168, 241, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 45, 49, 44, 102, 111, 111, 44, 116, 114, 117, 101, 10, 44, 98, 97, 114, 44, 102, 97, 108, 115, 101, 10, 50, 46, 53, 44, 98, 97, 122, 44, 116, 114, 117, 101, 10, 75, 182, 193, 80, 0, 0, 0, 235, 0, 0, 0, 67, 213, 243, 57, 141, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 53, 48, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 53, 48, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 51, 54, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 253, 105, 8, 216, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
			},
		},
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT COUNT(*) AS total_record_count from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0, 0, 0, 126, 0, 0, 0, 85, 56, 193, 36, 188, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 123, 34, 116, 111, 116, 97, 108, 95, 114, 101, 99, 111, 114, 100, 95, 99, 111, 117, 110, 116, 34, 58, 51, 125, 10, 196, 183, 134, 242, 0, 0, 0, 235, 0, 0, 0, 67, 213, 243, 57, 141, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 53, 48, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 53, 48, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 50, 53, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 47, 153, 24, 28, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
			},
		},
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT one from S3Object limit 1</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0x0, 0x0, 0x0, 0x68, 0x0, 0x0, 0x0, 0x55, 0xd7, 0x61, 0x46, 0x9e, 0xd, 0x3a, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74, 0xd, 0x3a, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x18, 0x61, 0x70, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x6f, 0x63, 0x74, 0x65, 0x74, 0x2d, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0xb, 0x3a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x7, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x2d, 0x31, 0xa, 0x17, 0xfb, 0x1, 0x90, 0x0, 0x0, 0x0, 0xea, 0x0, 0x0, 0x0, 0x43, 0xe8, 0x93, 0x10, 0x3d, 0xd, 0x3a, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74, 0xd, 0x3a, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x8, 0x74, 0x65, 0x78, 0x74, 0x2f, 0x78, 0x6d, 0x6c, 0xb, 0x3a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x53, 0x74, 0x61, 0x74, 0x73, 0x3c, 0x3f, 0x78, 0x6d, 0x6c, 0x20, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x3d, 0x22, 0x31, 0x2e, 0x30, 0x22, 0x20, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x69, 0x6e, 0x67, 0x3d, 0x22, 0x55, 0x54, 0x46, 0x2d, 0x38, 0x22, 0x3f, 0x3e, 0x3c, 0x53, 0x74, 0x61, 0x74, 0x73, 0x3e, 0x3c, 0x42, 0x79, 0x74, 0x65, 0x73, 0x53, 0x63, 0x61, 0x6e, 0x6e, 0x65, 0x64, 0x3e, 0x35, 0x30, 0x3c, 0x2f, 0x42, 0x79, 0x74, 0x65, 0x73, 0x53, 0x63, 0x61, 0x6e, 0x6e, 0x65, 0x64, 0x3e, 0x3c, 0x42, 0x79, 0x74, 0x65, 0x73, 0x50, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x64, 0x3e, 0x35, 0x30, 0x3c, 0x2f, 0x42, 0x79, 0x74, 0x65, 0x73, 0x50, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x64, 0x3e, 0x3c, 0x42, 0x79, 0x74, 0x65, 0x73, 0x52, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x65, 0x64, 0x3e, 0x33, 0x3c, 0x2f, 0x42, 0x79, 0x74, 0x65, 0x73, 0x52, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x65, 0x64, 0x3e, 0x3c, 0x2f, 0x53, 0x74, 0x61, 0x74, 0x73, 0x3e, 0x15, 0x72, 0x19, 0x94, 0x0, 0x0, 0x0, 0x38, 0x0, 0x0, 0x0, 0x28, 0xc1, 0xc6, 0x84, 0xd4, 0xd, 0x3a, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74, 0xb, 0x3a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x3, 0x45, 0x6e, 0x64, 0xcf, 0x97, 0xd3, 0x92,
			},
		},
	}

	var csvData = []byte(`one,two,three
-1,foo,true
,bar,false
2.5,baz,true
`)

	for i, testCase := range testTable {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			s3Select, err := NewS3Select(bytes.NewReader(testCase.requestXML))
			if err != nil {
				t.Fatal(err)
			}

			if err = s3Select.Open(func(offset, length int64) (io.ReadCloser, error) {
				return ioutil.NopCloser(bytes.NewReader(csvData)), nil
			}); err != nil {
				t.Fatal(err)
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()

			if !reflect.DeepEqual(w.response, testCase.expectedResult) {
				t.Errorf("received response does not match with expected reply\ngot: %#v\nwant:%#v", w.response, testCase.expectedResult)
			}
		})
	}

}

func TestJSONInput(t *testing.T) {

	var testTable = []struct {
		requestXML     []byte
		expectedResult []byte
	}{
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT one, two, three from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <JSON>
            <Type>DOCUMENT</Type>
        </JSON>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0, 0, 0, 137, 0, 0, 0, 85, 194, 213, 168, 241, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 45, 49, 44, 102, 111, 111, 44, 116, 114, 117, 101, 10, 44, 98, 97, 114, 44, 102, 97, 108, 115, 101, 10, 50, 46, 53, 44, 98, 97, 122, 44, 116, 114, 117, 101, 10, 75, 182, 193, 80, 0, 0, 0, 237, 0, 0, 0, 67, 90, 179, 204, 45, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 49, 49, 50, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 49, 49, 50, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 51, 54, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 181, 40, 50, 250, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
			},
		},
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT COUNT(*) AS total_record_count from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <JSON>
            <Type>DOCUMENT</Type>
        </JSON>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0, 0, 0, 103, 0, 0, 0, 85, 85, 49, 209, 79, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 51, 10, 175, 58, 213, 152, 0, 0, 0, 236, 0, 0, 0, 67, 103, 211, 229, 157, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 49, 49, 50, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 49, 49, 50, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 50, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 52, 192, 77, 114, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
			},
		},
	}

	var jsonData = []byte(`{"three":true,"two":"foo","one":-1}
{"three":false,"two":"bar","one":null}
{"three":true,"two":"baz","one":2.5}
`)

	for i, testCase := range testTable {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			s3Select, err := NewS3Select(bytes.NewReader(testCase.requestXML))
			if err != nil {
				t.Fatal(err)
			}

			if err = s3Select.Open(func(offset, length int64) (io.ReadCloser, error) {
				return ioutil.NopCloser(bytes.NewReader(jsonData)), nil
			}); err != nil {
				t.Fatal(err)
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()

			if !reflect.DeepEqual(w.response, testCase.expectedResult) {
				t.Errorf("received response does not match with expected reply\ngot: %s\nwant:%s", string(w.response), string(testCase.expectedResult))
			}
		})
	}
}

func TestParquetInput(t *testing.T) {

	var testTable = []struct {
		requestXML     []byte
		expectedResult []byte
	}{
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT one, two, three from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <Parquet>
        </Parquet>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0, 0, 0, 137, 0, 0, 0, 85, 194, 213, 168, 241, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 45, 49, 44, 102, 111, 111, 44, 116, 114, 117, 101, 10, 44, 98, 97, 114, 44, 102, 97, 108, 115, 101, 10, 50, 46, 53, 44, 98, 97, 122, 44, 116, 114, 117, 101, 10, 75, 182, 193, 80, 0, 0, 0, 235, 0, 0, 0, 67, 213, 243, 57, 141, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 45, 49, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 45, 49, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 51, 54, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 128, 96, 253, 66, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
			},
		},
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT COUNT(*) AS total_record_count from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <Parquet>
        </Parquet>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0, 0, 0, 103, 0, 0, 0, 85, 85, 49, 209, 79, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 51, 10, 175, 58, 213, 152, 0, 0, 0, 234, 0, 0, 0, 67, 232, 147, 16, 61, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 45, 49, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 45, 49, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 50, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 190, 146, 162, 21, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
			},
		},
	}

	for i, testCase := range testTable {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			getReader := func(offset int64, length int64) (io.ReadCloser, error) {
				testdataFile := "testdata.parquet"
				file, err := os.Open(testdataFile)
				if err != nil {
					return nil, err
				}

				fi, err := file.Stat()
				if err != nil {
					return nil, err
				}

				if offset < 0 {
					offset = fi.Size() + offset
				}

				if _, err = file.Seek(offset, os.SEEK_SET); err != nil {
					return nil, err
				}

				return file, nil
			}

			s3Select, err := NewS3Select(bytes.NewReader(testCase.requestXML))
			if err != nil {
				t.Fatal(err)
			}

			if err = s3Select.Open(getReader); err != nil {
				t.Fatal(err)
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()

			if !reflect.DeepEqual(w.response, testCase.expectedResult) {
				t.Errorf("received response does not match with expected reply\ngot: %#v\nwant:%#v", w.response, testCase.expectedResult)
			}
		})
	}
}
