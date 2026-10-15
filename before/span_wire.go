package before

// MarshalBinary concatenates the independently padded lower and upper Version
// encodings, matching the upstream Span format.
func (s Span) MarshalBinary() ([]byte, error) { return s.AppendBinary(nil), nil }

// AppendBinary appends the canonical encoding to dst and returns the result.
func (s Span) AppendBinary(dst []byte) []byte { return s.hi.AppendBinary(s.lo.AppendBinary(dst)) }

// EncodedLen returns the canonical byte length without producing the encoding.
func (s Span) EncodedLen() int { return s.lo.EncodedLen() + s.hi.EncodedLen() }

// DecodeSpan validates two canonical Versions and their causal order. Invalid
// endpoint encodings take precedence over ErrNonCanonical for crossed bounds.
func DecodeSpan(data []byte) (Span, error) {
	lo, end, err := decodeVersionPrefix(data)
	if err != nil {
		return Span{}, err
	}
	hi, err := DecodeVersion(data[end:])
	if err != nil {
		return Span{}, err
	}
	if !lo.LessEqual(hi) {
		return Span{}, ErrNonCanonical
	}
	if lo.Equal(hi) {
		hi = lo
	}
	return Span{lo: lo, hi: hi}, nil
}

// UnmarshalBinary replaces s only after complete validation.
func (s *Span) UnmarshalBinary(data []byte) error {
	decoded, err := DecodeSpan(data)
	if err != nil {
		return err
	}
	*s = decoded
	return nil
}
