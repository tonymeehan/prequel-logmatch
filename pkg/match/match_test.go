package match

import (
	"testing"
)

func TestMatchJson(t *testing.T) {

	m, err := makeMatchFunc("jq_json:.shrubbery")
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`{"shrubbery":"apple"}`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`{"nope":"apple"}`) {
		t.Errorf("Expected no match, got match.")
	}
}

func TestMatchJsonString(t *testing.T) {

	m, err := makeMatchFunc(`jq_json:select(.shrubbery == "apple")`)
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`{"shrubbery":"apple"}`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`{"shrubbery":"xapple"}`) {
		t.Errorf("Expected no match, got match.")
	}

}

func TestMatchJsonRegex(t *testing.T) {

	m, err := makeMatchFunc(`jq_json:.shrubbery | test("^a.")`)
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`{"shrubbery":"apple"}`) {
		t.Errorf("Expected match, got fail.")
	}

	if !m(`{"shrubbery":"applex"}`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`{"shrubbery":"banana"}`) {
		t.Errorf("Expected no match, got match.")
	}
}

func TestMatchYaml(t *testing.T) {

	m, err := makeMatchFunc("jq_yaml:.shrubbery")
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`shrubbery: apple`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`nope: apple`) {
		t.Errorf("Expected no match, got match.")
	}
}

var jsonData = `
{
  "widget": {
    "debug": "on",
    "window": {
      "title": "Sample Konfabulator Widget",
      "name": "main_window",
      "width": 500,
      "height": 500
    },
    "image": { 
      "src": "Images/Sun.png",
      "hOffset": 250,
      "vOffset": 250,
      "alignment": "center"
    },
    "text": {
      "data": "Click Here",
      "size": 36,
      "style": "bold",
      "vOffset": 100,
      "alignment": "center",
      "onMouseUp": "sun1.opacity = (sun1.opacity / 100) * 90;"
    }
  }
} `

func BenchmarkMatchJson(b *testing.B) {

	m1, _ := makeMatchFunc("jq_json:.widget.window.name")
	m2, _ := makeMatchFunc("jq_json:.widget.image.hOffset")
	m3, _ := makeMatchFunc("jq_json:.widget.text.onMouseUp")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m1(jsonData)
		m2(jsonData)
		m3(jsonData)
	}

}
