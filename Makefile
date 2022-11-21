MIN_COVERAGE = 70
test:
	go test -v -race -timeout 120s -count=1 -cover -coverprofile=coverage.txt && go tool cover -func=coverage.txt \
	| grep total | tee /dev/stderr | sed 's/\%//g' | awk '{err=0;c+=$$3}{if (c > 0 && c < $(MIN_COVERAGE)) {printf "=== FAIL: Coverage failed at %.2f%%\n", c; err=1}} END {exit err}'
