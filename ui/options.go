package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"sc/transport"
)

type Option struct {
	Label    string
	Value    *bool
	IntValue *int
	IntMin   int
	IntMax   int
	// GetRate/SetRate make the option a bandwidth limit in bytes/sec, stepped
	// through rateSteps by ←/→ instead of held in a local variable.
	GetRate func() int64
	SetRate func(int64)
}

// rateSteps is the ladder ←/→ walks for a bandwidth option; 0 is unlimited.
var rateSteps = []int64{
	0, 32 << 10, 64 << 10, 128 << 10, 256 << 10, 512 << 10,
	1 << 20, 2 << 20, 4 << 20, 8 << 20, 16 << 20, 32 << 20,
	64 << 20, 128 << 20, 256 << 20, 512 << 20, 1 << 30,
}

// stepRate moves cur one rung along rateSteps, snapping a value that came from
// -bwlimit and isn't on the ladder to the next rung in the direction of travel.
func stepRate(cur int64, delta int) int64 {
	if delta > 0 {
		for _, v := range rateSteps {
			if v > cur {
				return v
			}
		}
		return rateSteps[len(rateSteps)-1]
	}
	for i := len(rateSteps) - 1; i >= 0; i-- {
		if rateSteps[i] < cur {
			return rateSteps[i]
		}
	}
	return 0
}

type SettingsDialog struct {
	options []Option
	cursor  int
	visible bool
}

func NewSettingsDialog() *SettingsDialog {
	return &SettingsDialog{}
}

func (d *SettingsDialog) SetOptions(opts []Option) {
	d.options = opts
}

func (d *SettingsDialog) Open() {
	d.visible = true
	d.cursor = 0
}

func (d *SettingsDialog) Close() {
	d.visible = false
}

func (d *SettingsDialog) IsOpen() bool {
	return d.visible
}

func (d *SettingsDialog) MoveUp() {
	if d.cursor > 0 {
		d.cursor--
	}
}

func (d *SettingsDialog) MoveDown() {
	if d.cursor < len(d.options)-1 {
		d.cursor++
	}
}

func (d *SettingsDialog) UpdateChecksumLabel(algo string) {
	label := fmt.Sprintf("Checksum (%s)", algo)
	for i := range d.options {
		if strings.HasPrefix(d.options[i].Label, "Checksum") {
			d.options[i].Label = label
			break
		}
	}
}

func (d *SettingsDialog) Toggle() {
	if d.cursor < 0 || d.cursor >= len(d.options) {
		return
	}
	opt := &d.options[d.cursor]
	if opt.Value == nil {
		return
	}
	*opt.Value = !*opt.Value
}

func (d *SettingsDialog) Adjust(delta int) {
	if d.cursor < 0 || d.cursor >= len(d.options) {
		return
	}
	opt := &d.options[d.cursor]
	if opt.SetRate != nil {
		opt.SetRate(stepRate(opt.GetRate(), delta))
		return
	}
	if opt.IntValue == nil {
		return
	}
	v := *opt.IntValue + delta
	if v < opt.IntMin {
		v = opt.IntMin
	}
	if v > opt.IntMax {
		v = opt.IntMax
	}
	*opt.IntValue = v
}

var (
	styleDialogBorder = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("4")).
				Padding(1, 2)
	styleDialogTitle = lipgloss.NewStyle().Foreground(lipgloss.Color("4"))
	styleOptOn       = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	styleOptOff      = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	styleOptInt      = lipgloss.NewStyle().Foreground(lipgloss.Color("6"))
)

func (d *SettingsDialog) View(width, height int) string {
	if !d.visible {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(styleDialogTitle.Render("Settings"))
	sb.WriteString("\n\n")

	for i, opt := range d.options {
		marker := "  "
		if i == d.cursor {
			marker = "▶ "
		}
		var state string
		switch {
		case opt.GetRate != nil:
			state = styleOptInt.Render(fmt.Sprintf("[%4s]", transport.FormatRate(opt.GetRate())))
		case opt.IntValue != nil:
			state = styleOptInt.Render(fmt.Sprintf("[%4d]", *opt.IntValue))
		case *opt.Value:
			state = styleOptOn.Render("[  on]")
		default:
			state = styleOptOff.Render("[ off]")
		}
		sb.WriteString(fmt.Sprintf("%s%s  %s\n", marker, state, opt.Label))
	}

	sb.WriteString("\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Render("Space=toggle  ←/→=adjust  Esc=close"))

	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, styleDialogBorder.Render(sb.String()))
}
