import React, { useState } from 'react';
import { Document, Page, pdfjs } from 'react-pdf';

// Import the worker locally
import 'pdfjs-dist/build/pdf.worker.min.mjs';
pdfjs.GlobalWorkerOptions.workerSrc = new URL(
  'pdfjs-dist/build/pdf.worker.min.mjs',
  import.meta.url,
).toString();

export default function PDFViewer({ document, onClose }) {
    const [numPages, setNumPages] = useState(null);
    const [pageNumber, setPageNumber] = useState(1);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    if (!document) {
        return (
            <div className="flex-1 bg-gray-850 flex items-center justify-center text-gray-500">
                <div className="text-center">
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-16 h-16 mx-auto mb-4 opacity-50">
                        <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                    </svg>
                    <p className="text-sm">Select a document to view</p>
                </div>
            </div>
        );
    }

    const pdfUrl = `http://127.0.0.1:8000/api/documents/pdf/${encodeURIComponent(document.file_path)}`;

    function onDocumentLoadSuccess({ numPages }) {
        setNumPages(numPages);
        setLoading(false);
        setError(null);
    }

    function onDocumentLoadError(error) {
        console.error('Error loading PDF:', error);
        setError('Failed to load PDF. Please check if the file exists.');
        setLoading(false);
    }

    return (
        <div className="flex-1 bg-gray-850 flex flex-col border-r border-gray-700">
            {/* Header */}
            <div className="bg-gray-800 border-b border-gray-700 p-4 flex items-center justify-between">
                <div className="flex-1 min-w-0 mr-4">
                    <h2 className="text-sm font-semibold text-gray-100 truncate" title={document.title}>
                        {document.title || 'Untitled'}
                    </h2>
                    <p className="text-xs text-gray-500 truncate mt-1" title={document.file_path}>
                        {document.file_path}
                    </p>
                </div>
                <button
                    onClick={onClose}
                    className="p-2 hover:bg-gray-700 rounded-lg transition-colors flex-shrink-0"
                    title="Close PDF viewer"
                >
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-5 h-5 text-gray-400">
                        <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                    </svg>
                </button>
            </div>

            {/* Page Navigation */}
            {numPages && (
                <div className="bg-gray-800 border-b border-gray-700 px-4 py-2 flex items-center justify-center gap-4">
                    <button
                        onClick={() => setPageNumber(prev => Math.max(1, prev - 1))}
                        disabled={pageNumber <= 1}
                        className="p-1.5 hover:bg-gray-700 rounded disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    >
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor" className="w-4 h-4 text-gray-300">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
                        </svg>
                    </button>
                    <span className="text-sm text-gray-300">
                        Page {pageNumber} of {numPages}
                    </span>
                    <button
                        onClick={() => setPageNumber(prev => Math.min(numPages, prev + 1))}
                        disabled={pageNumber >= numPages}
                        className="p-1.5 hover:bg-gray-700 rounded disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    >
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor" className="w-4 h-4 text-gray-300">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                        </svg>
                    </button>
                </div>
            )}

            {/* PDF Viewer */}
            <div className="flex-1 overflow-auto bg-gray-900 flex items-start justify-center p-4">
                {loading && (
                    <div className="text-gray-400 text-sm mt-8">Loading PDF...</div>
                )}
                {error && (
                    <div className="text-red-400 text-sm mt-8">{error}</div>
                )}
                <Document
                    file={pdfUrl}
                    onLoadSuccess={onDocumentLoadSuccess}
                    onLoadError={onDocumentLoadError}

                >
                    <Page
                        pageNumber={pageNumber}
                        renderTextLayer={false}
                        renderAnnotationLayer={false}
                        className="shadow-lg"
                    />
                </Document>
            </div>
        </div>
    );
}
