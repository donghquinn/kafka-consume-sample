package util

import (
	"encoding/base64"
	"fmt"
	"math/big"
)

func ConvertStringToIntWithDecoding(encoded string, scale int) (*big.Rat, error) {
	// Base64 디코딩
	decodedBytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("base64 디코딩 오류: %v", err)
	}

	// 바이트 배열을 big.Int로 변환
	value := new(big.Int).SetBytes(decodedBytes)

	// big.Rat을 사용하여 소수점 위치 조정
	rat := new(big.Rat).SetFrac(value, big.NewInt(int64(10)).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))

	return rat, nil
}
