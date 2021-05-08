package cmd

import (
	"strconv"
)

type MingdaoVideo struct {
	Streams []stream   `json:"streams,omitempty"`
	Format  format   `json:"format,omitempty"`
}

type stream struct {
	Codec_type      string   `json:"codec_type,omitempty"`
	Codec_name      string    `json:"codec_name,omitempty"`
	Codec_long_name string     `json:"codec_long_name,omitempty"`
	Height          int     `json:"height,omitempty"`
	Width           int      `json:"width,omitempty"`
}

type format struct {
	Duration string   `json:"duration,omitempty"`
}

func formatMingdaoVideo(mediainfo *MediaInfo) *MingdaoVideo{
	Duration, _ := strconv.ParseFloat(mediainfo.General.Duration, 64)
	Duration = Duration / 1000
	duration := strconv.FormatFloat(Duration, 'f', -1, 64)
	format := format{Duration: duration}

	streams:=[]stream{}

	if mediainfo.Video.Format!=""{
		stream:=stream{Codec_type:"video",Width:mediainfo.Video.Width,Height:mediainfo.Video.Height,Codec_name:mediainfo.Video.Format,Codec_long_name:mediainfo.Video.Format_Info}
		streams=append(streams,stream)
	}

	mingdaoVideo:=MingdaoVideo{Streams:streams,Format:format}
	return &mingdaoVideo

}
