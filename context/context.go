// Package context exposes common environment utilities.
package context

// Context provides a bundle of the utilities available that can be easily passed around.
// Empty values are no-ops, so every utility may simply be called.
type Context struct {
	Log    Log
	Stats  Stats
	Config Config
}
