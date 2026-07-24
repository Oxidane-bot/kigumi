import { isAbsolute, posix, win32 } from "node:path";

export function checkedPath(raw) {
	if (typeof raw !== "string" || raw.length === 0 || raw.includes("\\") || raw.includes("\0")) {
		throw new Error("Kigumi tool paths must be non-empty POSIX paths");
	}
	if (isAbsolute(raw) || posix.isAbsolute(raw) || win32.isAbsolute(raw)) {
		throw new Error("Kigumi tools reject absolute paths");
	}
	const segments = raw.split("/");
	if (segments.includes("..")) {
		throw new Error("Kigumi tools reject parent traversal");
	}
	const firstEffectiveSegment = segments.find((segment) => segment !== "" && segment !== ".");
	if (firstEffectiveSegment?.toLowerCase() === ".kigumi") {
		throw new Error("Kigumi tools reject runtime-owned .kigumi paths");
	}
	return raw;
}
